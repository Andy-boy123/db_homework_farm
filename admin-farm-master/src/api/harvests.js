import request from '@/utils/request'

// admin-farm - 获取收获记录
export function getHarvest() {
  return request({
    url: '/get_harvests',
    method: 'get'
  })
}

// admin-farm - 添加收获记录
export function addHarvest(data) {
  return request({
    url: '/add_harvest',
    method: 'post',
    data
  })
}


// admin-farm - 删除收获记录
export function deleteHarvest(data) {
  return request({
    url: '/delete_harvest',
    method: 'post',
    data
  })
}

