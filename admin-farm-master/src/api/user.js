import request from '@/utils/request'

const prefix = process.env.NODE_ENV === 'development' ? '/cyan' : ''

export function login(data) {
  return request({
    url: '/vue-admin-template/user/login',
    method: 'post',
    data
  })
}

export function getInfo(token) {
  return request({
    url: '/vue-admin-template/user/info',
    method: 'get',
    params: { token }
  })
}

export function logout() {
  return request({
    url: '/vue-admin-template/user/logout',
    method: 'post'
  })
}

// admin-farm - 获取用户表
export function getAllUsers() {
  return request({
    url: '/get_all_users',
    method: 'get'
  })
}

// admin-farm - 修改用户
export function updateUserInfo(data) {
  return request({
    url: '/update_user_info',
    method: 'post',
    data
  })
}


// admin-farm - 用户注册
export function register(data) {
  return request({
    url: '/register',
    method: 'post',
    data
  })
}

// admin-farm - 用户登录
export function loginFarm(data) {
  return request({
    url: '/login',
    method: 'post',
    data
  })
}


// admin-farm - 用户注销
export function logoutFarm() {
  return request({
    url: '/logout',
    method: 'post'
  })
}