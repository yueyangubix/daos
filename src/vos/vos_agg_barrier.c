/**
 * (C) Copyright 2016-2025 Intel Corporation.
 *
 * SPDX-License-Identifier: GPL-2.0-or-later
 */

/**
 * VOS Aggregation Barrier Implementation
 *
 * Barrier management functions used by the VOS module.
 */

#define D_LOGFAC	DD_FAC(vos)

#include <daos/common.h>
#include <daos_srv/vos.h>
#include "vos_agg_barrier.h"
#include "vos_internal.h"

void
agg_barriers_init(struct agg_barriers *barriers)
{
	barriers->ab_epochs = NULL;
	barriers->ab_nr = 0;
	barriers->ab_alloc_nr = 0;
}

void
agg_barriers_fini(struct agg_barriers *barriers)
{
	if (barriers->ab_epochs != NULL) {
		D_FREE(barriers->ab_epochs);
		barriers->ab_epochs = NULL;
	}
	barriers->ab_nr = 0;
	barriers->ab_alloc_nr = 0;
}

int
agg_barriers_add(struct agg_barriers *barriers, daos_epoch_t epoch)
{
	void	*tmp;

	if (barriers->ab_nr >= barriers->ab_alloc_nr) {
		uint32_t	new_nr = barriers->ab_alloc_nr == 0 ? 4 : barriers->ab_alloc_nr * 2;

		D_REALLOC(tmp, barriers->ab_epochs, barriers->ab_alloc_nr * sizeof(daos_epoch_t),
			  new_nr * sizeof(daos_epoch_t));
		if (tmp == NULL)
			return -DER_NOMEM;
		barriers->ab_epochs = tmp;
		barriers->ab_alloc_nr = new_nr;
	}
	barriers->ab_epochs[barriers->ab_nr++] = epoch;
	return 0;
}
