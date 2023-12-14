import request from '@/utils/request'

// admin-farm - 获取作物表
export function getCropTypes() {
  return request({
    url: '/get_crop_types',
    method: 'get'
  })
}

// admin-farm - 添加作物
export function addCropType(data) {
  return request({
    url: '/add_crop_type',
    method: 'post',
    data
  })
}


// admin-farm - 删除作物
export function deleteCropType(data) {
  return request({
    url: '/delete_crop_type',
    method: 'post',
    data
  })
}

