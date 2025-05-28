"""
  (C) Copyright 2025 Hewlett Packard Enterprise Development LP

  SPDX-License-Identifier: BSD-2-Clause-Patent
"""
import time

from apricot import TestWithServers


class RbldAutoRecoveryPolicy(TestWithServers):
    """Rebuild test cases featuring IOR.

    This class contains tests for pool rebuild that feature I/O going on
    during the rebuild using IOR.

    :avocado: recursive
    """

    def test_rebuild_auto_recovery_policy(self):
        """Jira ID: DAOS-17420.

        Test Description: Verify Rebuild Auto Recovery Policy

        :avocado: tags=all,full_regression
        :avocado: tags=vm
        :avocado: tags=pool,rebuild
        :avocado: tags=RbldAutoRecoveryPolicy,test_rebuild_auto_recovery_policy
        """
        self.log_step('Setup pool')
        pool = self.get_pool(connect=False)
        dmg = self.get_dmg_command()

        # TODO calculate this
        # The detection delay shall be a couple of SWIM periods + SWIM suspicion timeout
        # + CRT_EVENT_DELAY + some margin of error
        detection_delay = 30

        # SCENARIO 1: System Creation and default self_heal
        self.log_step('Verify default self_heal policy')
        # TODO the test plan says "exclude;pool_exclude;pool_rebuild"
        # but the actual is "exclude;rebuild"
        self.__verify_prop_self_heal(pool, 'exclude;rebuild')

        # SCENARIO 2: Disabling and Enabling Self-Heal
        self.log_step('Disable self_heal')
        pool.set_prop('self_heal:none')

        self.log_step('Stop a rank and verify there are no exclusions or rebuild')
        all_ranks = list(self.server_managers[0].ranks.keys())
        ranks_x = self.random.sample(all_ranks, k=1)
        # ranks_y = self.random.sample(list(set(all_ranks) - set(ranks_x)), k=1)
        dmg.system_stop(ranks=ranks_x)
        self.log.info('Waiting for detection delay of %s seconds', detection_delay)
        time.sleep(detection_delay)
        pool.verify_query(expected_response={'disabled_ranks': [], 'rebuild': 'idle'})
        dmg.system_query()
        # TODO verify no rebuild

        # TODO Enable self-heal and invoke dmg system self-heal eval
        #      Rank X excluded from system and pool
        #      Rank X rebuild in the pool
        # TODO Invoke dmg system stop --ranks=Y and wait for the detection delay.
        #      Rank Y excluded from system and pool
        #      Rank Y rebuilding in the pool

        return

    def __verify_prop_self_heal(self, pool, expected_value):
        """Verify the self_heal property of the pool.

        Args:
            pool (TestPool): The pool to check.
            expected_value (str): The expected self_heal property value.

        """
        response = pool.get_prop(name='self_heal')['response']
        actual_value = response[0]['value']
        if actual_value != expected_value:
            self.fail(f'Expected self_heal policy to be {expected_value}, but got {actual_value}')
