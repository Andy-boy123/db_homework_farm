<template>
  <div class="app-container">
    <!-- 管理员 管理员 -->
    <template v-if="roles[0] === 2 || roles[0] === 999">
      <admin />
    </template>
    <!-- 农户 -->
    <template v-if="roles[0] === 0">
      <peasant />
    </template>
    <!-- 粮企 -->
    <template v-if="roles[0] === 1">
      <grain />
    </template>
  </div>
</template>
<script>
import { mapGetters } from 'vuex'
import admin from './components/admin'
import peasant from './components/peasant'
import grain from './components/grain'
export default {
  name: 'Sales',
  components: {
    admin,
    peasant,
    grain
  },
  computed: {
    ...mapGetters([
      'roles'
    ])
  }
};
</script>
  