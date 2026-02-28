/**
 * (C) Copyright 2016-2025 Intel Corporation.
 *
 * SPDX-License-Identifier: BSD-2-Clause-Patent
 */

/**
 * DAOS Aggregation Barrier Module - Public Definitions
 *
 * This header contains the public definitions for the aggregation barrier
 * module that are shared between the object and vos modules.
 */

#ifndef __DAOS_SRV_AGG_BARRIER_H__
#define __DAOS_SRV_AGG_BARRIER_H__

#include <daos/common.h>
#include <daos_srv/vos.h>

#define AGG_BARRIER_MAGIC	0x41474242
#define AGG_BARRIER_PREFIX	"_agg_barrier_"
#define AGG_BARRIER_PREFIX_LEN	(sizeof(AGG_BARRIER_PREFIX) - 1)

enum barrier_state {
	BARRIER_STATE_ACTIVE = 0,
	BARRIER_STATE_LOCAL_STALE,
	BARRIER_STATE_GLOBAL_STALE
};

struct agg_barrier {
	uint32_t	abb_magic;
	daos_epoch_t	abb_epoch;
};

struct agg_barriers {
	daos_epoch_t	*ab_epochs;
	uint32_t	 ab_nr;
	uint32_t	 ab_alloc_nr;
};

static inline bool
agg_is_barrier_akey(daos_key_t *akey)
{
	if (akey->iov_len > AGG_BARRIER_PREFIX_LEN &&
	    memcmp(akey->iov_buf, AGG_BARRIER_PREFIX, AGG_BARRIER_PREFIX_LEN) == 0)
		return true;
	return false;
}

static inline daos_epoch_t
agg_get_barrier_epoch(daos_key_t *akey)
{
	char		*epoch_str;
	daos_epoch_t	 epoch;

	epoch_str = (char *)akey->iov_buf + AGG_BARRIER_PREFIX_LEN;
	epoch = strtoull(epoch_str, NULL, 10);
	return epoch;
}

/*
 * Function declarations - implementations are split between vos and object
 * modules according to their dependencies
 */

/* Basic barrier list management - implemented in vos/vos_agg_barrier.c */
void
agg_barriers_init(struct agg_barriers *barriers);

void
agg_barriers_fini(struct agg_barriers *barriers);

int
agg_barriers_add(struct agg_barriers *barriers, daos_epoch_t epoch);

/* Object module specific functions - implemented in object/agg_barrier.c */
int
agg_get_prev_barrier(daos_unit_oid_t oid, daos_key_t *dkey,
		     daos_handle_t cont_hdl, daos_epoch_t target_barrier,
		     daos_epoch_t *prev_barrier);

int
agg_send_barrier_rpc(daos_epoch_t barrier_epoch, daos_key_t *dkey,
		     daos_unit_oid_t oid, daos_handle_t coh);

bool
barrier_has_newer(daos_unit_oid_t oid, daos_key_t *dkey,
		  daos_handle_t cont_hdl, daos_epoch_t check_epoch);

bool
barrier_has_newer_parity(daos_unit_oid_t oid, daos_key_t *dkey,
			 daos_handle_t cont_hdl, daos_epoch_t check_epoch);

bool
barrier_confirm_global_stale(daos_unit_oid_t oid, daos_key_t *dkey,
			     daos_epoch_t barrier_epoch,
			     daos_handle_t cont_hdl, bool is_parity);

int
barrier_delete(daos_unit_oid_t oid, daos_key_t *dkey,
	       daos_epoch_t barrier_epoch, daos_handle_t cont_hdl);

int
barrier_cleanup_barriers(struct agg_barriers *barriers,
			daos_unit_oid_t oid,
			daos_key_t *dkey,
			daos_handle_t cont_hdl,
			bool is_parity);

int
agg_get_all_barriers(daos_unit_oid_t oid, daos_key_t *dkey,
		      daos_handle_t cont_hdl, struct agg_barriers *barriers);

int
agg_barrier_cleanup(daos_unit_oid_t oid, daos_handle_t coh,
		    daos_key_t *dkey);

#endif /* __DAOS_SRV_AGG_BARRIER_H__ */
