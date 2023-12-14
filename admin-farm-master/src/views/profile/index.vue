<template>
  <div class="user-profile">
    <div class="user-info">
      <div class="user-info-item">
        <label class="label">用户名：</label>
        <span class="value">{{ userInfo.username }}</span>
      </div>
      <div class="user-info-item">
        <label class="label">上次登录时间：</label>
        <span class="value">{{ lastLoginTime }}</span>
      </div>
      <div class="user-info-item">
        <label class="label">邮箱：</label>
        <span class="value">{{ userInfo.email }}</span>
      </div>
      <div class="user-info-item">
        <label class="label">手机号：</label>
        <span class="value">{{ userInfo.phonenumber }}</span>
      </div>
      <div class="user-info-item">
        <label class="label">组织名：</label>
        <span class="value">{{ userInfo.organization_name }}</span>
      </div>
      <div class="user-info-item">
        <label class="label">用户组：</label>
        <span class="value">{{ userInfo.organization_type }}</span>
      </div>
    </div>
  </div>
</template>
<script>
import { mapGetters } from "vuex";
import moment from 'moment'

export default {
  name: "Profile",
  computed: {
    ...mapGetters(["userInfo"]),
    lastLoginTime() {
      const originalDateString = this.userInfo.last_login
      const momentDate = moment(originalDateString, "ddd, DD MMM YYYY HH:mm:ss [GMT]");

      const formattedDateString = momentDate.format("YYYY-MM-D HH:mm:ss");
      return formattedDateString
    }
  },
};
</script>

<style>

.user-profile {
  padding: 20px;
  border: 1px solid #eaeaea;
  border-radius: 4px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.user-info {
  display: flex;
  flex-direction: column;
}

.user-info-item {
  display: flex;
  margin-bottom: 24px;
  align-items: center;
}

.label {
  width: 120px;
  font-weight: bold;
}

.value {
  flex-grow: 1;
}
</style>
