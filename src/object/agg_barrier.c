/**
 * (C) Copyright 2016-2025 Intel Corporation.
 *
 * SPDX-License-Identifier: GPL-2.0-or-later
 */

/**
 * DAOS Aggregation Barrier Module
 *
 * src/object/agg_barrier.c
 *
 * Stale Barrier 清理流程：
 * 1. 获取 dkey 下所有 barriers (按 epoch 排序)
 * 2. 检查 Local Stale：
 *    - 数据片：存在更新的 barrier
 *    - 校验片：存在更新的 barrier 且 有新 parity 数据依附于新 barrier
 * 3. 确认 Global Stale：
 *    - 校验片：向 shard group 中所有校验片查询，都认为是 LOCAL_STALE
 *    - 数据片：向所有校验片查询，都认为非 ACTIVE
 * 4. 删除 Global Stale 的 barriers
 */

#define D_LOGFAC	DD_FAC(vos)

#include <daos/common.h>
#include <daos_srv/vos.h>
#include <daos_srv/pool.h>
#include <daos_srv/container.h>
#include <daos_srv/daos_engine.h>
#include <daos/object.h>
#include <daos/container.h>
#include <daos/placement.h>
#include <daos/pool.h>
#include "agg_barrier.h"
#include "obj_ec.h"
#include "obj_internal.h"

struct agg_barrier_rpc_ud {
	daos_epoch_t		 abru_barrier_epoch;
	daos_key_t		*abru_dkey;
	daos_unit_oid_t		 abru_oid;
	daos_handle_t		 abru_coh;
	struct ds_pool		*abru_pool;
	uint32_t		 abru_grp_idx;
	uint32_t		 abru_success_count;
	uint32_t		 abru_expected_count;
	ABT_eventual		 abru_eventual;
	int			 abru_rc;
};

static void
agg_barrier_send_ult(void *arg)
{
	struct agg_barrier_rpc_ud	*ud = arg;
	struct daos_oclass_attr		*oca;
	daos_handle_t			 obj_hdl = DAOS_HDL_INVAL;
	struct pl_obj_layout		*layout = NULL;
	struct daos_shard_loc		peer_shards[OBJ_EC_MAX_P];
	crt_endpoint_t			tgt_ep = { 0 };
	crt_rpc_t			*rpc = NULL;
	struct obj_agg_barrier_in	*ab_in = NULL;
	struct obj_agg_barrier_out	*ab_out = NULL;
	uint32_t			grp_start;
	uint32_t			peer;
	uint32_t			shard_idx;
	struct pl_obj_shard		*p_shard;
	struct pool_target		*tgt;
	int				rc = 0;
	char				barrier_akey_name[64];
	daos_key_t			barrier_akey;
	daos_handle_t			 cont_hdl;
	uuid_t				 cont_uuid, coh_uuid;
	struct pl_map			*map = NULL;
	struct daos_obj_md		 md = { 0 };
	struct cont_props		 props;

	cont_hdl = ud->abru_coh;

	rc = dc_cont_hdl2uuid(cont_hdl, &coh_uuid, &cont_uuid);
	if (rc != 0) {
		D_ERROR("dc_cont_hdl2uuid failed: "DF_RC"\n", DP_RC(rc));
		D_GOTO(out, rc);
	}
	oca = daos_oclass_attr_find(ud->abru_oid.id_pub, NULL);
	if (oca == NULL) {
		D_ERROR("daos_oclass_attr_find failed\n");
		D_GOTO(out, rc = -DER_PROTO);
	}

	if (!daos_oclass_is_ec(oca)) {
		D_ERROR("Not an EC object\n");
		D_GOTO(out, rc = -DER_PROTO);
	}

	if (ud->abru_oid.id_shard < obj_ec_data_tgt_nr(oca)) {
		D_ERROR("Not an EC parity shard\n");
		D_GOTO(out, rc = -DER_PROTO);
	}

	rc = dsc_obj_open(cont_hdl, ud->abru_oid.id_pub, DAOS_OO_RW, &obj_hdl);
	if (rc) {
		D_ERROR("dsc_obj_open failed: "DF_RC"\n", DP_RC(rc));
		goto out;
	}

	/* Get object layout using placement API */
	map = pl_map_find(ud->abru_pool->sp_uuid, ud->abru_oid.id_pub);
	if (map == NULL) {
		D_ERROR("Failed to find pool map\n");
		D_GOTO(out_obj, rc = -DER_INVAL);
	}

	props = dc_cont_hdl2props(cont_hdl);
	md.omd_id = ud->abru_oid.id_pub;
	md.omd_ver = ud->abru_pool->sp_map_version;
	md.omd_fdom_lvl = props.dcp_redun_lvl;
	md.omd_pdom_lvl = props.dcp_perf_domain;
	md.omd_pda = props.dcp_ec_pda;
	rc = pl_obj_place(map, ud->abru_oid.id_layout_ver, &md, DAOS_OO_RW, NULL, &layout);
	pl_map_decref(map);
	if (rc) {
		D_ERROR("pl_obj_place failed: "DF_RC"\n", DP_RC(rc));
		goto out_obj;
	}

	grp_start = (ud->abru_oid.id_shard / daos_oclass_grp_size(oca)) *
		    layout->ol_grp_size;

	ud->abru_expected_count = 0;
	ud->abru_success_count = 0;
	for (peer = 0; peer < layout->ol_grp_size; peer++) {
		shard_idx = grp_start + peer;
		p_shard = &layout->ol_shards[shard_idx];
		rc = pool_map_find_target(ud->abru_pool->sp_map, p_shard->po_target, &tgt);
		if (rc == 1) {
			peer_shards[peer].sd_rank = tgt->ta_comp.co_rank;
			peer_shards[peer].sd_tgt_idx = tgt->ta_comp.co_index;
			ud->abru_expected_count++;
			rc = 0;
		} else {
			peer_shards[peer].sd_rank = DAOS_TGT_IGNORE;
		}
	}

	snprintf(barrier_akey_name, sizeof(barrier_akey_name), "%s"DF_U64,
		 AGG_BARRIER_PREFIX, ud->abru_barrier_epoch);
	d_iov_set(&barrier_akey, barrier_akey_name, strlen(barrier_akey_name));

	for (peer = 0; peer < layout->ol_grp_size; peer++) {
		if (peer_shards[peer].sd_rank == DAOS_TGT_IGNORE)
			continue;

		tgt_ep.ep_rank = peer_shards[peer].sd_rank;
		tgt_ep.ep_tag = peer_shards[peer].sd_tgt_idx;

		rc = obj_req_create(dss_get_module_info()->dmi_ctx, &tgt_ep,
				       DAOS_OBJ_RPC_AGG_BARRIER, &rpc);
		if (rc) {
			D_ERROR("obj_req_create failed for peer %u: "DF_RC"\n",
				peer, DP_RC(rc));
			if (ud->abru_rc == 0)
				ud->abru_rc = rc;
			continue;
		}

		ab_in = crt_req_get(rpc);
		uuid_copy(ab_in->ab_pool_uuid, ud->abru_pool->sp_uuid);
		uuid_copy(ab_in->ab_cont_uuid, cont_uuid);
		uuid_copy(ab_in->ab_coh_uuid, coh_uuid);
		ab_in->ab_oid = ud->abru_oid;
		ab_in->ab_dkey = *ud->abru_dkey;
		ab_in->ab_barrier_epoch = ud->abru_barrier_epoch;
		ab_in->ab_map_ver = ud->abru_pool->sp_map_version;

		rc = dss_rpc_send(rpc);
		if (rc) {
			D_ERROR("dss_rpc_send failed for peer %u: "DF_RC"\n",
				peer, DP_RC(rc));
			if (ud->abru_rc == 0)
				ud->abru_rc = rc;
			crt_req_decref(rpc);
			rpc = NULL;
			continue;
		}

		ab_out = crt_reply_get(rpc);
		rc = ab_out->ab_status;
		if (rc != 0) {
			D_ERROR("Peer %u failed barrier creation: "DF_RC"\n",
				peer, DP_RC(rc));
			if (ud->abru_rc == 0)
				ud->abru_rc = rc;
		} else {
			ud->abru_success_count++;
		}

		crt_req_decref(rpc);
		rpc = NULL;
	}

	pl_obj_layout_free(layout);
out_obj:
	if (daos_handle_is_valid(obj_hdl))
		dsc_obj_close(obj_hdl);
out:
	ds_pool_put(ud->abru_pool);
	if (ud->abru_success_count < ud->abru_expected_count && ud->abru_rc == 0)
		ud->abru_rc = -DER_IO;
	ABT_eventual_set(ud->abru_eventual, &ud->abru_rc, sizeof(ud->abru_rc));
}

int
agg_send_barrier_rpc(daos_epoch_t barrier_epoch, daos_key_t *dkey,
		     daos_unit_oid_t oid, daos_handle_t coh)
{
	struct agg_barrier_rpc_ud	ud = { 0 };
	struct daos_oclass_attr		*oca;
	struct ds_pool			*pool;
	struct ds_cont_hdl		*cont_hdl;
	uuid_t				 coh_uuid, cont_uuid;
	int				*status;
	int				rc;

	oca = daos_oclass_attr_find(oid.id_pub, NULL);
	if (oca == NULL) {
		D_ERROR("daos_oclass_attr_find failed\n");
		return -DER_PROTO;
	}

	if (!daos_oclass_is_ec(oca)) {
		D_ERROR("Not an EC object\n");
		return -DER_PROTO;
	}

	if (oid.id_shard < obj_ec_data_tgt_nr(oca)) {
		D_ERROR("Not an EC parity shard\n");
		return -DER_PROTO;
	}

	rc = dc_cont_hdl2uuid(coh, &coh_uuid, &cont_uuid);
	if (rc != 0) {
		D_ERROR("dc_cont_hdl2uuid failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	cont_hdl = ds_cont_hdl_lookup(coh_uuid);
	if (cont_hdl == NULL) {
		D_ERROR("ds_cont_hdl_lookup failed\n");
		return -DER_INVAL;
	}

	rc = ds_pool_lookup(cont_hdl->sch_cont->sc_pool_uuid, &pool);
	ds_cont_hdl_put(cont_hdl);
	if (rc != 0) {
		D_ERROR("ds_pool_lookup failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	ud.abru_barrier_epoch = barrier_epoch;
	ud.abru_dkey = dkey;
	ud.abru_oid = oid;
	ud.abru_coh = coh;
	ud.abru_pool = pool;
	ud.abru_success_count = 0;
	ud.abru_expected_count = 0;
	ud.abru_rc = 0;

	rc = ABT_eventual_create(sizeof(ud.abru_rc), &ud.abru_eventual);
	if (rc != ABT_SUCCESS) {
		rc = dss_abterr2der(rc);
		ds_pool_put(pool);
		return rc;
	}

	rc = dss_ult_create(agg_barrier_send_ult, &ud,
			    DSS_XS_VOS, 0, 0, NULL);
	if (rc) {
		D_ERROR("dss_ult_create failed: "DF_RC"\n", DP_RC(rc));
		ABT_eventual_free(&ud.abru_eventual);
		ds_pool_put(pool);
		return rc;
	}

	rc = ABT_eventual_wait(ud.abru_eventual, (void **)&status);
	if (rc != ABT_SUCCESS) {
		rc = dss_abterr2der(rc);
		ABT_eventual_free(&ud.abru_eventual);
		return rc;
	}

	if (*status != 0)
		rc = *status;

	ABT_eventual_free(&ud.abru_eventual);

	return rc;
}

struct agg_prev_barrier_ud {
	daos_epoch_t	vabpu_target_barrier;
	daos_epoch_t	vabpu_prev_barrier;
	bool		vabpu_found_target;
};

static int
agg_prev_barrier_cb(daos_handle_t ih, vos_iter_entry_t *entry,
		    vos_iter_type_t type, vos_iter_param_t *param,
		    void *arg, unsigned int *unused)
{
	struct agg_prev_barrier_ud *ud = arg;
	daos_epoch_t	epoch;

	if (!agg_is_barrier_akey(&entry->ie_key))
		return 0;

	epoch = agg_get_barrier_epoch(&entry->ie_key);

	if (epoch == ud->vabpu_target_barrier) {
		ud->vabpu_found_target = true;
		return 0;
	}

	if (epoch < ud->vabpu_target_barrier && epoch > ud->vabpu_prev_barrier)
		ud->vabpu_prev_barrier = epoch;

	return 0;
}

int
agg_get_prev_barrier(daos_unit_oid_t oid, daos_key_t *dkey,
		     daos_handle_t cont_hdl, daos_epoch_t target_barrier,
		     daos_epoch_t *prev_barrier)
{
	vos_iter_param_t	iter_param = { 0 };
	struct vos_iter_anchors	anchors = { 0 };
	struct agg_prev_barrier_ud ud = { 0 };
	int			rc;

	iter_param.ip_hdl = cont_hdl;
	iter_param.ip_oid = oid;
	iter_param.ip_dkey = *dkey;

	ud.vabpu_target_barrier = target_barrier;
	ud.vabpu_prev_barrier = 0;
	ud.vabpu_found_target = false;

	rc = vos_iterate(&iter_param, VOS_ITER_AKEY, false, &anchors,
			 agg_prev_barrier_cb, NULL, &ud, NULL);
	if (rc != 0) {
		D_ERROR("vos_iterate failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	if (!ud.vabpu_found_target)
		return -DER_NONEXIST;

	*prev_barrier = ud.vabpu_prev_barrier;
	return 0;
}

struct agg_get_all_barriers_ud {
	struct agg_barriers	*agabu_barriers;
};

static int
agg_get_all_barriers_cb(daos_handle_t ih, vos_iter_entry_t *entry,
			vos_iter_type_t type, vos_iter_param_t *param,
			void *arg, unsigned int *unused)
{
	struct agg_get_all_barriers_ud	*ud = arg;
	daos_epoch_t			 epoch;

	if (!agg_is_barrier_akey(&entry->ie_key))
		return 0;

	epoch = agg_get_barrier_epoch(&entry->ie_key);

	return agg_barriers_add(ud->agabu_barriers, epoch);
}

int
agg_get_all_barriers(daos_unit_oid_t oid, daos_key_t *dkey,
		      daos_handle_t cont_hdl, struct agg_barriers *barriers)
{
	vos_iter_param_t	iter_param = { 0 };
	struct vos_iter_anchors	anchors = { 0 };
	struct agg_get_all_barriers_ud ud = { 0 };
	int			rc;

	iter_param.ip_hdl = cont_hdl;
	iter_param.ip_oid = oid;
	iter_param.ip_dkey = *dkey;

	ud.agabu_barriers = barriers;

	rc = vos_iterate(&iter_param, VOS_ITER_AKEY, false, &anchors,
			 agg_get_all_barriers_cb, NULL, &ud, NULL);
	if (rc != 0) {
		D_ERROR("vos_iterate failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	return 0;
}

/* ============================================================================
 * Stale Barrier 检测与清理
 * ============================================================================ */

/* 检查是否存在 epoch 大于 check_epoch 的 barrier */
struct barrier_has_newer_ud {
	daos_epoch_t	 bhn_check_epoch;
	bool		 bhn_has_newer;
};

static int
barrier_has_newer_cb(daos_handle_t ih, vos_iter_entry_t *entry,
		     vos_iter_type_t type, vos_iter_param_t *param,
		     void *arg, unsigned int *unused)
{
	struct barrier_has_newer_ud	*ud = arg;
	daos_epoch_t			 epoch;

	if (!agg_is_barrier_akey(&entry->ie_key))
		return 0;

	epoch = agg_get_barrier_epoch(&entry->ie_key);

	if (epoch > ud->bhn_check_epoch) {
		ud->bhn_has_newer = true;
		return 1;  /* 提前结束遍历 */
	}

	return 0;
}

/* 检查是否存在 epoch 大于 check_epoch 的 barrier（供 RPC handler 使用） */
bool
barrier_has_newer(daos_unit_oid_t oid, daos_key_t *dkey,
		  daos_handle_t cont_hdl, daos_epoch_t check_epoch)
{
	vos_iter_param_t	iter_param = { 0 };
	struct vos_iter_anchors	anchors = { 0 };
	struct barrier_has_newer_ud ud = { 0 };
	int			rc;

	iter_param.ip_hdl = cont_hdl;
	iter_param.ip_oid = oid;
	iter_param.ip_dkey = *dkey;

	ud.bhn_check_epoch = check_epoch;
	ud.bhn_has_newer = false;

	rc = vos_iterate(&iter_param, VOS_ITER_AKEY, false, &anchors,
			 barrier_has_newer_cb, NULL, &ud, NULL);
	if (rc != 0 && rc != 1) {
		D_ERROR("vos_iterate failed: "DF_RC"\n", DP_RC(rc));
		return false;
	}

	return ud.bhn_has_newer;
}

/*
 * 检查是否存在 epoch 大于 check_epoch 的 parity 数据
 *
 * 注意：parity 数据存储在 akey "0" 中，与副本数据相同，
 * 区别仅在于 recx.rx_idx 带有 PARITY_INDICATOR 标记
 */
struct barrier_has_parity_ud {
	daos_epoch_t	 bhp_epoch_start;
	bool		 bhp_has_parity;
};

static int
barrier_has_parity_cb(daos_handle_t ih, vos_iter_entry_t *entry,
		      vos_iter_type_t type, vos_iter_param_t *param,
		      void *arg, unsigned int *unused)
{
	struct barrier_has_parity_ud	*ud = arg;

	/* 关键：识别 parity 数据 - 通过 PARITY_INDICATOR 标记 */
	if (entry->ie_recx.rx_idx & PARITY_INDICATOR) {
		if (entry->ie_epoch > ud->bhp_epoch_start)
			ud->bhp_has_parity = true;
	}

	return 0;
}

/* 检查是否存在 epoch 大于 check_epoch 的 parity 数据（供 RPC handler 使用） */
bool
barrier_has_newer_parity(daos_unit_oid_t oid, daos_key_t *dkey,
			 daos_handle_t cont_hdl, daos_epoch_t check_epoch)
{
	vos_iter_param_t	iter_param = { 0 };
	struct vos_iter_anchors	anchors = { 0 };
	struct barrier_has_parity_ud ud = { 0 };
	daos_key_t		akey_zero;
	int			rc;

	/* 设置 dkey 和 akey（"0"） */
	iter_param.ip_hdl = cont_hdl;
	iter_param.ip_oid = oid;
	iter_param.ip_dkey = *dkey;
	d_iov_set(&akey_zero, "0", 1);
	iter_param.ip_akey = akey_zero;

	ud.bhp_epoch_start = check_epoch;
	ud.bhp_has_parity = false;

	rc = vos_iterate(&iter_param, VOS_ITER_RECX, false, &anchors,
			 barrier_has_parity_cb, NULL, &ud, NULL);
	if (rc != 0) {
		D_ERROR("vos_iterate failed: "DF_RC"\n", DP_RC(rc));
		return false;
	}

	return ud.bhp_has_parity;
}

/*
 * 检查 Local Stale 状态
 *
 * 根据设计文档：
 * - 数据片：存在更新的 Barrier（调用者已确保不是最新的 barrier）
 * - 校验片：存在更新的 Barrier 且 有新的校验数据依附于新 Barrier
 *
 * 注意：此函数在 barrier_cleanup_stale_barriers 中调用，
 *       传入的 barrier 已经确保不是最新的（数组中最后一个）
 */
static bool
barrier_check_local_stale(daos_unit_oid_t oid, daos_key_t *dkey,
			  daos_handle_t cont_hdl, daos_epoch_t barrier_epoch,
			  bool is_parity)
{
	/* 数据片：肯定有更新的 barrier，直接返回 true */
	if (!is_parity)
		return true;

	/* 校验片：需要检查是否有新 parity 数据依附于新 barrier */
	return barrier_has_newer_parity(oid, dkey, cont_hdl, barrier_epoch);
}

/* ============================================================================
 * Global Stale 确认 - 向 shard group 发送查询 RPC
 * ============================================================================ */

struct barrier_global_stale_ud {
	daos_epoch_t		 bgsu_barrier_epoch;
	daos_key_t		*bgsu_dkey;
	daos_unit_oid_t		 bgsu_oid;
	daos_handle_t		 bgsu_coh;
	struct ds_pool		*bgsu_pool;
	/* 结果统计 */
	uint32_t		 bgsu_expected_count;    /* 其他 parity shard 数量 */
	uint32_t		 bgsu_parity_stale_count; /* 认为是 LOCAL_STALE 的数量 */
	ABT_eventual		 bgsu_eventual;
	int			 bgsu_rc;
};

/* 构建 shard group 的目标列表，仅包含校验片（跳过自身） */
static int
barrier_build_parity_targets(daos_unit_oid_t oid, struct ds_pool *pool,
			     daos_handle_t cont_hdl,
			     struct daos_shard_loc *parity_shards,
			     uint32_t *parity_count)
{
	struct daos_oclass_attr	*oca;
	struct pl_obj_layout	*layout = NULL;
	struct pl_obj_shard	*p_shard;
	struct pool_target	*tgt;
	uint32_t		grp_start, grp_size;
	uint32_t		data_tgt_nr;
	uint32_t		peer, shard_idx;
	int			rc;
	struct pl_map		*map = NULL;
	struct daos_obj_md	 md = { 0 };
	struct cont_props	 props;

	oca = daos_oclass_attr_find(oid.id_pub, NULL);
	if (oca == NULL || !daos_oclass_is_ec(oca))
		return -DER_PROTO;

	/* 使用 placement API 获取对象布局 */
	map = pl_map_find(pool->sp_uuid, oid.id_pub);
	if (map == NULL) {
		D_ERROR("Failed to find pool map\n");
		return -DER_INVAL;
	}

	props = dc_cont_hdl2props(cont_hdl);
	md.omd_id = oid.id_pub;
	md.omd_ver = pool->sp_map_version;
	md.omd_fdom_lvl = props.dcp_redun_lvl;
	md.omd_pdom_lvl = props.dcp_perf_domain;
	md.omd_pda = props.dcp_ec_pda;
	rc = pl_obj_place(map, oid.id_layout_ver, &md, DAOS_OO_RW, NULL, &layout);
	pl_map_decref(map);
	if (rc) {
		D_ERROR("pl_obj_place failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	grp_size = daos_oclass_grp_size(oca);
	grp_start = (oid.id_shard / grp_size) * layout->ol_grp_size;
	data_tgt_nr = obj_ec_data_tgt_nr(oca);

	*parity_count = 0;
	for (peer = 0; peer < layout->ol_grp_size; peer++) {
		shard_idx = grp_start + peer;
		/* 只收集校验片 */
		if (shard_idx % grp_size < data_tgt_nr)
			continue;

		/* 跳过自身（当前 shard） */
		if (shard_idx == oid.id_shard)
			continue;

		p_shard = &layout->ol_shards[shard_idx];
		rc = pool_map_find_target(pool->sp_map, p_shard->po_target, &tgt);
		if (rc == 1) {
			parity_shards[*parity_count].sd_rank = tgt->ta_comp.co_rank;
			parity_shards[*parity_count].sd_tgt_idx = tgt->ta_comp.co_index;
			(*parity_count)++;
		}
	}

	pl_obj_layout_free(layout);
	return rc;
}

static void
barrier_global_stale_query_ult(void *arg)
{
	struct barrier_global_stale_ud	*ud = arg;
	struct daos_shard_loc		 parity_shards[OBJ_EC_MAX_P];
	struct daos_oclass_attr		*oca;
	crt_endpoint_t			tgt_ep = { 0 };
	crt_rpc_t			*rpc = NULL;
	struct obj_barrier_state_query_in	*bsq_in = NULL;
	struct obj_barrier_state_query_out	*bsq_out = NULL;
	uint32_t			parity_count = 0;
	uint32_t			i;
	int				rc = 0;
	uuid_t				 cont_uuid, coh_uuid;

	rc = dc_cont_hdl2uuid(ud->bgsu_coh, &coh_uuid, &cont_uuid);
	if (rc != 0) {
		D_ERROR("dc_cont_hdl2uuid failed: "DF_RC"\n", DP_RC(rc));
		D_GOTO(out, rc);
	}

	oca = daos_oclass_attr_find(ud->bgsu_oid.id_pub, NULL);
	if (oca == NULL || !daos_oclass_is_ec(oca)) {
		D_ERROR("Not an EC object\n");
		D_GOTO(out, rc = -DER_PROTO);
	}

	/* 构建 shard group 中的校验片目标列表 */
	rc = barrier_build_parity_targets(ud->bgsu_oid, ud->bgsu_pool,
					  ud->bgsu_coh,
					  parity_shards, &parity_count);
	if (rc) {
		D_ERROR("Failed to build parity targets: "DF_RC"\n", DP_RC(rc));
		goto out;
	}

	ud->bgsu_expected_count = parity_count;
	ud->bgsu_parity_stale_count = 0;

	/* 向所有校验片发送状态查询 RPC */
	for (i = 0; i < parity_count; i++) {
		tgt_ep.ep_rank = parity_shards[i].sd_rank;
		tgt_ep.ep_tag = parity_shards[i].sd_tgt_idx;

		rc = obj_req_create(dss_get_module_info()->dmi_ctx, &tgt_ep,
				       DAOS_OBJ_RPC_BARRIER_STATE_QUERY, &rpc);
		if (rc) {
			D_ERROR("obj_req_create failed for parity %u: "DF_RC"\n",
				i, DP_RC(rc));
			if (ud->bgsu_rc == 0)
				ud->bgsu_rc = rc;
			continue;
		}

		bsq_in = crt_req_get(rpc);
		uuid_copy(bsq_in->bsq_pool_uuid, ud->bgsu_pool->sp_uuid);
		uuid_copy(bsq_in->bsq_cont_uuid, cont_uuid);
		uuid_copy(bsq_in->bsq_coh_uuid, coh_uuid);
		bsq_in->bsq_oid = ud->bgsu_oid;
		bsq_in->bsq_dkey = *ud->bgsu_dkey;
		bsq_in->bsq_barrier_epoch = ud->bgsu_barrier_epoch;
		bsq_in->bsq_map_ver = ud->bgsu_pool->sp_map_version;
		bsq_in->bsq_is_parity = 1;  /* 查询校验片 */

		rc = dss_rpc_send(rpc);
		if (rc) {
			D_ERROR("dss_rpc_send failed for parity %u: "DF_RC"\n",
				i, DP_RC(rc));
			if (ud->bgsu_rc == 0)
				ud->bgsu_rc = rc;
			crt_req_decref(rpc);
			rpc = NULL;
			continue;
		}

		bsq_out = crt_reply_get(rpc);
		rc = bsq_out->bsq_status;
		if (rc != 0) {
			D_ERROR("Parity shard %u failed state query: "DF_RC"\n",
				i, DP_RC(rc));
			if (ud->bgsu_rc == 0)
				ud->bgsu_rc = rc;
		} else {
			/* 统计认为是 LOCAL_STALE 的 parity shard 数量 */
			if (bsq_out->bsq_state == BARRIER_STATE_LOCAL_STALE)
				ud->bgsu_parity_stale_count++;
		}

		crt_req_decref(rpc);
		rpc = NULL;
	}

out:
	ds_pool_put(ud->bgsu_pool);
	ABT_eventual_set(ud->bgsu_eventual, &ud->bgsu_rc, sizeof(ud->bgsu_rc));
}

/*
 * 确认 Global Stale 状态
 *
 * Global Stale 判定条件：shard group 内所有 parity shard 都认为是 LOCAL_STALE
 *
 * 实现方式：
 * - 向 shard group 中除自身外的所有 parity shard 发送查询
 * - 如果都返回 LOCAL_STALE，则该 barrier 为 GLOBAL_STALE
 *
 * 注意：调用者（如果是 parity shard）自身已经是 LOCAL_STALE 才会调用此函数，
 *      所以只需要确认其他 parity shard 的状态即可。
 */
bool
barrier_confirm_global_stale(daos_unit_oid_t oid, daos_key_t *dkey,
			     daos_epoch_t barrier_epoch, daos_handle_t cont_hdl,
			     bool is_parity __attribute__((unused)))
{
	struct barrier_global_stale_ud	 ud = { 0 };
	struct daos_oclass_attr		*oca;
	struct ds_pool			*pool;
	struct ds_cont_hdl		*cont_hdl_struct;
	uuid_t				 coh_uuid, cont_uuid;
	int				*status;
	int				rc;
	bool				is_global_stale = false;

	oca = daos_oclass_attr_find(oid.id_pub, NULL);
	if (oca == NULL || !daos_oclass_is_ec(oca)) {
		D_ERROR("Not an EC object\n");
		return false;
	}

	rc = dc_cont_hdl2uuid(cont_hdl, &coh_uuid, &cont_uuid);
	if (rc != 0) {
		D_ERROR("dc_cont_hdl2uuid failed: "DF_RC"\n", DP_RC(rc));
		return false;
	}

	cont_hdl_struct = ds_cont_hdl_lookup(coh_uuid);
	if (cont_hdl_struct == NULL) {
		D_ERROR("ds_cont_hdl_lookup failed\n");
		return false;
	}

	rc = ds_pool_lookup(cont_hdl_struct->sch_cont->sc_pool_uuid, &pool);
	ds_cont_hdl_put(cont_hdl_struct);
	if (rc != 0) {
		D_ERROR("ds_pool_lookup failed: "DF_RC"\n", DP_RC(rc));
		return false;
	}

	ud.bgsu_barrier_epoch = barrier_epoch;
	ud.bgsu_dkey = dkey;
	ud.bgsu_oid = oid;
	ud.bgsu_coh = cont_hdl;
	ud.bgsu_pool = pool;
	ud.bgsu_rc = 0;

	rc = ABT_eventual_create(sizeof(ud.bgsu_rc), &ud.bgsu_eventual);
	if (rc != ABT_SUCCESS) {
		rc = dss_abterr2der(rc);
		ds_pool_put(pool);
		return false;
	}

	rc = dss_ult_create(barrier_global_stale_query_ult, &ud,
			    DSS_XS_VOS, 0, 0, NULL);
	if (rc) {
		D_ERROR("dss_ult_create failed: "DF_RC"\n", DP_RC(rc));
		ABT_eventual_free(&ud.bgsu_eventual);
		ds_pool_put(pool);
		return false;
	}

	rc = ABT_eventual_wait(ud.bgsu_eventual, (void **)&status);
	if (rc != ABT_SUCCESS) {
		rc = dss_abterr2der(rc);
		ABT_eventual_free(&ud.bgsu_eventual);
		ds_pool_put(pool);
		return false;
	}

	ABT_eventual_free(&ud.bgsu_eventual);
	ds_pool_put(pool);

	if (ud.bgsu_rc != 0) {
		D_ERROR("barrier_global_stale_query failed: "DF_RC"\n", DP_RC(ud.bgsu_rc));
		return false;
	}

	/*
	 * Global Stale 判定：shard group 内所有 parity shard 都认为是 LOCAL_STALE
	 *
	 * 注意：当前 shard（如果是 parity shard）自身已经是 LOCAL_STALE 才会走到这里，
	 *      所以只需要确认其他 parity shard 也都认为是 LOCAL_STALE 即可。
	 *      expected_count 是不包含自身的其他 parity shard 数量。
	 */
	is_global_stale = (ud.bgsu_parity_stale_count == ud.bgsu_expected_count);
	D_DEBUG(DB_EPC, "Global stale check: stale_count=%u, other_parity=%u, result=%d\n",
		ud.bgsu_parity_stale_count, ud.bgsu_expected_count, is_global_stale);

	return is_global_stale;
}

/* 删除指定 epoch 的 barrier */
int
barrier_delete(daos_unit_oid_t oid, daos_key_t *dkey,
	       daos_epoch_t barrier_epoch, daos_handle_t cont_hdl)
{
	char		barrier_akey_name[64];
	daos_key_t	barrier_akey;
	int		rc;

	snprintf(barrier_akey_name, sizeof(barrier_akey_name), "%s"DF_U64,
		 AGG_BARRIER_PREFIX, barrier_epoch);
	d_iov_set(&barrier_akey, barrier_akey_name, strlen(barrier_akey_name));

	/* 使用 punch 删除 barrier akey */
	rc = vos_obj_punch(cont_hdl, oid, DAOS_EPOCH_MAX, 0, 0, dkey, 1, &barrier_akey, NULL);
	if (rc) {
		D_ERROR("Failed to punch barrier epoch "DF_U64": "DF_RC"\n",
			barrier_epoch, DP_RC(rc));
		return rc;
	}

	D_DEBUG(DB_EPC, "Deleted barrier epoch "DF_U64"\n", barrier_epoch);
	return 0;
}

/*
 * 清理单个 stale barrier
 *
 * 注意：调用者已确保此 barrier 不是最新的（有更新的 barrier 存在）
 *
 * 返回：0 - 未清理（非 stale 或确认失败），1 - 已清理，负数 - 错误
 */
static int
barrier_cleanup_one(daos_unit_oid_t oid, daos_key_t *dkey,
		    daos_epoch_t barrier_epoch, daos_handle_t cont_hdl,
		    bool is_parity)
{
	bool is_local_stale = false;
	bool is_global_stale = false;
	int rc;

	/* 步骤 1: 检查 Local Stale */
	is_local_stale = barrier_check_local_stale(oid, dkey, cont_hdl,
						   barrier_epoch, is_parity);
	if (!is_local_stale) {
		D_DEBUG(DB_EPC, "Barrier epoch "DF_U64" is not local stale\n",
			barrier_epoch);
		return 0;
	}

	D_DEBUG(DB_EPC, "Barrier epoch "DF_U64" is local stale, "
		"checking global stale...\n", barrier_epoch);

	/* 步骤 2: 确认 Global Stale */
	is_global_stale = barrier_confirm_global_stale(oid, dkey, barrier_epoch,
						       cont_hdl, is_parity);
	if (!is_global_stale) {
		D_DEBUG(DB_EPC, "Barrier epoch "DF_U64" is not global stale\n",
			barrier_epoch);
		return 0;
	}

	D_DEBUG(DB_EPC, "Barrier epoch "DF_U64" is global stale, deleting...\n",
		barrier_epoch);

	/* 步骤 3: 删除 barrier */
	rc = barrier_delete(oid, dkey, barrier_epoch, cont_hdl);
	if (rc) {
		D_ERROR("Failed to delete barrier epoch "DF_U64": "DF_RC"\n",
			barrier_epoch, DP_RC(rc));
		return rc;
	}

	D_DEBUG(DB_EPC, "Successfully cleaned up stale barrier epoch "DF_U64"\n",
		barrier_epoch);
	return 1;
}

/*
 * 批量清理 stale barriers
 *
 * 逻辑：遍历 barriers 数组（已按 epoch 排序），对于每个非最新的 barrier：
 * - 数据片：有更新的 barrier 就是 local stale
 * - 校验片：有更新的 barrier 且新 barrier 有 parity 数据依附才是 local stale
 * - Local stale 确认后，再发送 RPC 确认 global stale
 * - 最新的 barrier（数组最后一个）始终保留
 */
static int
barrier_cleanup_stale_barriers(struct agg_barriers *barriers,
			       daos_unit_oid_t oid, daos_key_t *dkey,
			       daos_handle_t cont_hdl, bool is_parity)
{
	uint32_t i;
	int rc;
	int cleaned = 0;

	if (barriers->ab_nr <= 1) {
		D_DEBUG(DB_EPC, "Only %u barrier(s), skip cleanup\n", barriers->ab_nr);
		return 0;
	}

	D_DEBUG(DB_EPC, "Checking %u barriers for cleanup (is_parity=%d)\n",
		barriers->ab_nr, is_parity);

	/*
	 * 遍历非最新的 barriers（即除了最后一个之外的所有 barrier）
	 * 这些 barrier 都有更新的 barrier 存在
	 */
	for (i = 0; i < barriers->ab_nr - 1; i++) {
		rc = barrier_cleanup_one(oid, dkey, barriers->ab_epochs[i],
					 cont_hdl, is_parity);
		if (rc < 0) {
			D_ERROR("Failed to cleanup barrier epoch "DF_U64": "DF_RC"\n",
				barriers->ab_epochs[i], DP_RC(rc));
			/* 出错时继续处理下一个 */
		} else if (rc == 1) {
			/* 已清理，标记为 0 */
			barriers->ab_epochs[i] = 0;
			cleaned++;
		}
		/* rc == 0 表示未清理（非 stale），保留原值 */
	}

	/* 压缩数组：移除已清理的 barriers（标记为 0 的）*/
	if (cleaned > 0) {
		uint32_t j = 0;
		for (i = 0; i < barriers->ab_nr; i++) {
			if (barriers->ab_epochs[i] != 0)
				barriers->ab_epochs[j++] = barriers->ab_epochs[i];
		}
		barriers->ab_nr = j;
		D_DEBUG(DB_EPC, "Cleaned up %u stale barriers, %u remaining\n",
			cleaned, barriers->ab_nr);
	}

	return cleaned;
}

/*
 * Aggregation Barrier 清理入口函数
 *
 * 流程：
 * 1. 获取当前 dkey 下的所有 barriers
 * 2. 按 epoch 排序（agg_get_all_barriers 返回的已排序）
 * 3. 对每个非最新的 barrier，检查是否 stale：
 *    a. Local Stale：数据片检查新 barrier，校验片还要检查新 parity
 *    b. Global Stale：向 shard group 的校验片发送查询 RPC 确认
 * 4. 删除确认为 global stale 的 barriers
 */
int
agg_barrier_cleanup(daos_unit_oid_t oid, daos_handle_t coh,
		    daos_key_t *dkey)
{
	struct daos_oclass_attr	*oca;
	struct agg_barriers	 barriers = { 0 };
	uint32_t		 data_tgt_nr;
	bool			 is_parity;
	int			 rc;

	/* 获取 object class 属性 */
	oca = daos_oclass_attr_find(oid.id_pub, NULL);
	if (oca == NULL || !daos_oclass_is_ec(oca)) {
		D_ERROR("Not an EC object\n");
		return -DER_PROTO;
	}

	/* 判断当前 shard 类型：数据片 or 校验片 */
	data_tgt_nr = obj_ec_data_tgt_nr(oca);
	is_parity = (oid.id_shard % daos_oclass_grp_size(oca)) >= data_tgt_nr;

	agg_barriers_init(&barriers);

	/* 步骤 1: 获取所有 barriers */
	rc = agg_get_all_barriers(oid, dkey, coh, &barriers);
	if (rc != 0) {
		D_ERROR("Failed to get all barriers: "DF_RC"\n", DP_RC(rc));
		goto out_fini;
	}

	if (barriers.ab_nr == 0) {
		D_DEBUG(DB_EPC, "No barriers found for cleanup\n");
		goto out_fini;
	}

	D_DEBUG(DB_EPC, "Found %u barriers for cleanup, shard=%u, is_parity=%d\n",
		barriers.ab_nr, oid.id_shard, is_parity);

	/* 步骤 2-4: 检查并清理 stale barriers */
	rc = barrier_cleanup_stale_barriers(&barriers, oid, dkey, coh, is_parity);
	if (rc < 0) {
		D_ERROR("Failed to cleanup stale barriers: "DF_RC"\n", DP_RC(rc));
		goto out_fini;
	}

	D_DEBUG(DB_EPC, "Barrier cleanup completed, cleaned %d barriers\n", rc);
	rc = 0;

out_fini:
	agg_barriers_fini(&barriers);
	return rc;
}
