/**
 * (C) Copyright 2016-2025 Intel Corporation.
 *
 * SPDX-License-Identifier: GPL-2.0-or-later
 */

#include <daos/container.h>
#include <daos/object.h>
#include <daos/common.h>
#include <daos/checksum.h>
#include <daos/pool_map.h>
#include <daos/btree.h>
#include <daos_srv/vos.h>
#include <daos_srv/daos_engine.h>
// #include <daos_srv/ec.h>
#include <daos_srv/pool.h>
#include <gurt/heap.h>
#include <gurt/list.h>
#include "obj_ec.h"
#include "srv_internal.h"
#include "obj_rpc.h"
#include "agg_barrier.h"

int
ec_parity_get_prev_barrier(daos_unit_oid_t oid, daos_key_t *dkey,
			   daos_handle_t cont_hdl, daos_epoch_t target_barrier,
			   daos_epoch_t *prev_barrier)
{
	return agg_get_prev_barrier(oid, dkey, cont_hdl, target_barrier, prev_barrier);
}

struct ec_parity_stripe_info_ud {
	unsigned char		**epsi_bufs;
	uint64_t		*epsi_covered;
	uint64_t		epsi_stripe_size;
	uint64_t		epsi_stripe_count;
	daos_epoch_t		epsi_barrier_epoch;
	daos_epoch_t		epsi_prev_barrier;
	daos_size_t		epsi_rec_size;
};

static int
ec_parity_stripe_collect_cb(daos_handle_t ih, vos_iter_entry_t *entry,
			    vos_iter_type_t type, vos_iter_param_t *param,
			    void *arg, unsigned int *unused)
{
	struct ec_parity_stripe_info_ud *ud = arg;
	uint64_t stripe_idx;
	daos_off_t copy_offset;
	uint64_t copy_size;

	if (entry->ie_epoch > ud->epsi_barrier_epoch)
		return 0;
	if (entry->ie_epoch <= ud->epsi_prev_barrier)
		return 0;

	if (entry->ie_recx.rx_idx & PARITY_INDICATOR)
		return 0;

	/* Capture record size from first entry */
	if (ud->epsi_rec_size == 0)
		ud->epsi_rec_size = entry->ie_rsize;

	stripe_idx = entry->ie_recx.rx_idx / ud->epsi_stripe_size;
	if (stripe_idx >= ud->epsi_stripe_count)
		return 0;

	copy_offset = entry->ie_recx.rx_idx % ud->epsi_stripe_size;
	copy_size = entry->ie_recx.rx_nr;

	if (entry->ie_biov.bi_buf != NULL) {
		memcpy(ud->epsi_bufs[stripe_idx] + copy_offset,
		       entry->ie_biov.bi_buf,
		       copy_size);
	}
	ud->epsi_covered[stripe_idx] += copy_size;

	return 0;
}

static int
ec_parity_stripe_collect_vos(daos_handle_t coh, daos_unit_oid_t oid,
			     daos_key_t *dkey, daos_epoch_t barrier_epoch,
			     daos_epoch_t prev_barrier, uint64_t stripe_size,
			     uint64_t stripe_count, unsigned char **bufs,
			     uint64_t *covered, daos_size_t *rec_size)
{
	vos_iter_param_t	iter_param = { 0 };
	struct vos_iter_anchors	anchors = { 0 };
	struct ec_parity_stripe_info_ud ud = { 0 };
	int			rc;

	iter_param.ip_hdl = coh;
	iter_param.ip_oid = oid;
	iter_param.ip_dkey = *dkey;

	ud.epsi_bufs = bufs;
	ud.epsi_covered = covered;
	ud.epsi_stripe_size = stripe_size;
	ud.epsi_stripe_count = stripe_count;
	ud.epsi_barrier_epoch = barrier_epoch;
	ud.epsi_prev_barrier = prev_barrier;
	ud.epsi_rec_size = 0;

	rc = vos_iterate(&iter_param, VOS_ITER_RECX, false, &anchors,
			 ec_parity_stripe_collect_cb, NULL, &ud, NULL);
	if (rc != 0) {
		D_ERROR("vos_iterate failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	*rec_size = ud.epsi_rec_size;
	return 0;
}

struct ec_parity_max_offset_ud {
	daos_epoch_t		epmu_barrier_epoch;
	daos_epoch_t		epmu_prev_barrier;
	daos_off_t		epmu_max_offset;
};

static int
ec_parity_max_offset_cb(daos_handle_t ih, vos_iter_entry_t *entry,
			vos_iter_type_t type, vos_iter_param_t *param,
			void *arg, unsigned int *unused)
{
	struct ec_parity_max_offset_ud *ud = arg;
	daos_off_t recx_end;

	if (entry->ie_epoch > ud->epmu_barrier_epoch)
		return 0;
	if (entry->ie_epoch <= ud->epmu_prev_barrier)
		return 0;

	if (entry->ie_recx.rx_idx & PARITY_INDICATOR)
		return 0;

	recx_end = entry->ie_recx.rx_idx + entry->ie_recx.rx_nr - 1;
	if (recx_end > ud->epmu_max_offset)
		ud->epmu_max_offset = recx_end;

	return 0;
}

static int
ec_parity_get_max_offset(daos_handle_t coh, daos_unit_oid_t oid,
			 daos_key_t *dkey, daos_epoch_t barrier_epoch,
			 daos_epoch_t prev_barrier, daos_off_t *max_offset)
{
	vos_iter_param_t	iter_param = { 0 };
	struct vos_iter_anchors	anchors = { 0 };
	struct ec_parity_max_offset_ud ud = { 0 };
	int			rc;

	iter_param.ip_hdl = coh;
	iter_param.ip_oid = oid;
	iter_param.ip_dkey = *dkey;

	ud.epmu_barrier_epoch = barrier_epoch;
	ud.epmu_prev_barrier = prev_barrier;
	ud.epmu_max_offset = -1;

	rc = vos_iterate(&iter_param, VOS_ITER_RECX, false, &anchors,
			 ec_parity_max_offset_cb, NULL, &ud, NULL);
	if (rc != 0) {
		D_ERROR("vos_iterate failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	*max_offset = ud.epmu_max_offset;
	return 0;
}

static int
ec_parity_stripe_collect_all(daos_handle_t coh, daos_unit_oid_t oid,
			     daos_key_t *dkey, daos_epoch_t barrier_epoch,
			     daos_epoch_t prev_barrier, uint64_t stripe_size,
			     unsigned char ***bufs, uint64_t **covered,
			     uint64_t *stripe_count, daos_size_t *rec_size)
{
	daos_off_t		 max_offset;
	uint64_t		 count;
	unsigned char		**stripe_bufs = NULL;
	uint64_t		 *stripe_cov = NULL;
	int			 i;
	int			 rc = 0;

	rc = ec_parity_get_max_offset(coh, oid, dkey, barrier_epoch,
				      prev_barrier, &max_offset);
	if (rc != 0) {
		D_ERROR("Get max offset failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	count = (max_offset + 1) / stripe_size + 1;

	D_ALLOC_ARRAY(stripe_bufs, count);
	if (stripe_bufs == NULL)
		return -DER_NOMEM;

	D_ALLOC_ARRAY(stripe_cov, count);
	if (stripe_cov == NULL) {
		D_FREE(stripe_bufs);
		return -DER_NOMEM;
	}

	for (i = 0; i < count; i++) {
		D_ALLOC(stripe_bufs[i], stripe_size);
		if (stripe_bufs[i] == NULL) {
			while (--i >= 0)
				D_FREE(stripe_bufs[i]);
			D_FREE(stripe_bufs);
			D_FREE(stripe_cov);
			return -DER_NOMEM;
		}
		memset(stripe_bufs[i], 0, stripe_size);
	}

	rc = ec_parity_stripe_collect_vos(coh, oid, dkey, barrier_epoch,
					  prev_barrier, stripe_size, count,
					  stripe_bufs, stripe_cov, rec_size);
	if (rc != 0) {
		for (i = 0; i < count; i++)
			D_FREE(stripe_bufs[i]);
		D_FREE(stripe_bufs);
		D_FREE(stripe_cov);
		return rc;
	}

	*bufs = stripe_bufs;
	*covered = stripe_cov;
	*stripe_count = count;

	return 0;
}

static int
ec_parity_write_vos(daos_handle_t coh, daos_unit_oid_t oid,
		    daos_key_t *dkey, d_iov_t *val_iov, struct daos_oclass_attr *oca,
		    uint64_t stripe_idx, daos_epoch_t epoch, unsigned char *buf,
		    daos_size_t rec_size)
{
	daos_iod_t	iod = { 0 };
	d_sg_list_t	sgl = { 0 };
	daos_key_t	akey_zero;
	int		rc;

	d_iov_set(&akey_zero, "0", 1);
	iod.iod_name = akey_zero;
	iod.iod_nr = 1;
	iod.iod_recxs = &(daos_recx_t) {
		.rx_idx = PARITY_INDICATOR | (stripe_idx * obj_ec_stripe_rec_nr(oca)),
		.rx_nr = obj_ec_stripe_rec_nr(oca)
	};
	iod.iod_type = DAOS_IOD_ARRAY;
	iod.iod_size = rec_size;

	sgl.sg_nr = 1;
	sgl.sg_iovs = val_iov;

	rc = vos_obj_update(coh, oid, epoch, 0, 0, dkey, 1, &iod, NULL, &sgl);
	if (rc != 0)
		D_ERROR("vos_obj_update failed: "DF_RC"\n", DP_RC(rc));

	return rc;
}

struct ec_parity_create_ud {
	daos_handle_t		epu_coh;
	daos_unit_oid_t		epu_oid;
	daos_key_t		*epu_dkey;
	daos_epoch_t		epu_epoch;
	daos_epoch_t		epu_prev_barrier;
};

static void
ec_parity_create_ult(void *arg)
{
	struct ec_parity_create_ud	*parity_ud = arg;
	struct daos_oclass_attr		*oca;
	unsigned char			**stripe_bufs = NULL;
	unsigned char			*parity_buf = NULL;
	uint64_t			*stripe_covered = NULL;
	uint64_t			stripe_size;
	uint64_t			cell_bytes;
	uint64_t			stripe_count = 0;
	daos_size_t			rec_size = 0;
	uint64_t			i;
	int				rc = 0;

	oca = daos_oclass_attr_find(parity_ud->epu_oid.id_pub, NULL);
	if (oca == NULL || !daos_oclass_is_ec(oca)) {
		D_ERROR("Not an EC object\n");
		return;
	}

	if (!is_ec_parity_shard_by_tgt_off(parity_ud->epu_oid.id_shard %
			daos_oclass_grp_size(oca), oca)) {
		D_ERROR("Not an EC parity shard\n");
		return;
	}

	stripe_size = obj_ec_stripe_rec_nr(oca);
	cell_bytes = (uint64_t)oca->u.ec.e_len;

	rc = ec_parity_stripe_collect_all(parity_ud->epu_coh, parity_ud->epu_oid,
					  parity_ud->epu_dkey, parity_ud->epu_epoch,
					  parity_ud->epu_prev_barrier, stripe_size,
					  &stripe_bufs, &stripe_covered, &stripe_count,
					  &rec_size);
	if (rc != 0) {
		D_ERROR("Collect stripe data failed: "DF_RC"\n", DP_RC(rc));
		goto out_stripe;
	}

	if (stripe_count == 0) {
		D_DEBUG(DB_EPC, "No stripe data to process\n");
		goto out_stripe;
	}

	D_ALLOC(parity_buf, cell_bytes);
	if (parity_buf == NULL) {
		D_ERROR("Allocate parity buffer failed\n");
		rc = -DER_NOMEM;
		goto out_stripe;
	}

	for (i = 0; i < stripe_count; i++) {
		if (stripe_covered[i] < stripe_size)
			continue;

		rc = obj_ec_encode_buf(parity_ud->epu_oid.id_pub, oca, rec_size,
				       stripe_bufs[i], (unsigned char **)&parity_buf);
		if (rc != 0) {
			D_ERROR("EC encode failed for stripe %lu: "DF_RC"\n", i, DP_RC(rc));
			continue;
		}

		rc = ec_parity_write_vos(parity_ud->epu_coh, parity_ud->epu_oid,
					 parity_ud->epu_dkey, NULL, oca, i,
					 parity_ud->epu_epoch, parity_buf, rec_size);
		if (rc != 0)
			D_ERROR("Write parity failed for stripe %lu: "DF_RC"\n", i, DP_RC(rc));
	}

	D_FREE(parity_buf);
out_stripe:
	for (i = 0; i < stripe_count; i++)
		D_FREE(stripe_bufs[i]);
	D_FREE(stripe_bufs);
	D_FREE(stripe_covered);
}

int
ec_parity_create(daos_epoch_t barrier_epoch, daos_key_t *dkey,
		 daos_unit_oid_t oid, daos_handle_t coh)
{
	struct ec_parity_create_ud	 parity_ud = {0};
	daos_epoch_t			 prev_barrier;
	int				 rc = 0;

	rc = agg_send_barrier_rpc(barrier_epoch, dkey, oid, coh);
	if (rc != 0) {
		D_ERROR("Send barrier RPC failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	rc = ec_parity_get_prev_barrier(oid, dkey, coh, barrier_epoch,
					&prev_barrier);
	if (rc == -DER_NONEXIST) {
		D_ERROR("Target barrier "DF_U64" not found\n", barrier_epoch);
		return rc;
	}
	if (rc != 0) {
		D_ERROR("Get prev barrier failed: "DF_RC"\n", DP_RC(rc));
		return rc;
	}

	parity_ud.epu_epoch = barrier_epoch;
	parity_ud.epu_prev_barrier = prev_barrier;
	parity_ud.epu_dkey = dkey;
	parity_ud.epu_coh = coh;
	parity_ud.epu_oid = oid;

	return dss_ult_create(ec_parity_create_ult, &parity_ud,
			      DSS_XS_VOS, 0, 0, NULL);
}
