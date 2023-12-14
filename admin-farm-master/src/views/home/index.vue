<template>
  <div class="dashboard-container">
    <div class="dashboard-text">username: {{ userName }}</div>
  </div>
</template>
<script>

import { mapGetters } from 'vuex'

export default {
  name: 'Home',
  computed: {
    ...mapGetters([
      'userName'
    ])
  }
}
</script>

<style lang="scss" scoped>
.dashboard {
  &-container {
    margin: 30px;
  }

  &-text {
    font-size: 30px;
    line-height: 46px;
  }
}
</style>

