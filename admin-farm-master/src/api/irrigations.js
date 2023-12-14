import request from '@/utils/request'

// admin-farm - 获取灌溉记录
export function getIrrigation() {
  return request({
    url: '/get_irrigations',
    method: 'get'
  })
}

// admin-farm - 添加灌溉记录
export function addIrrigation(data) {
  return request({
    url: '/add_irrigation',
    method: 'post',
    data
  })
}


// admin-farm - 删除灌溉记录
export function deleteIrrigation(data) {
  return request({
    url: '/delete_irrigation',
    method: 'post',
    data
  })
}

