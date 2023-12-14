import request from '@/utils/request'

// admin-farm - 获取用人单位
export function getOrganizations() {
  return request({
    url: '/get_organizations',
    method: 'get'
  })
}

// admin-farm - 添加用人单位
export function addOrganizations(data) {
  return request({
    url: '/add_organization',
    method: 'post',
    data
  })
}


// admin-farm - 删除用人单位
export function deleteOrganizations(data) {
  return request({
    url: '/delete_organization',
    method: 'post',
    data
  })
}

