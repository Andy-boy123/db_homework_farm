
module.exports = [

  // admin-farm - 获取作物表
  {
    url: '/get_crop_types',
    type: 'get',
    response: config => {
      return {
        "crop_types": [
          {
            "crop_name": "小麦",
            "crop_type_id": 1
          },
          {
            "crop_name": "大麦",
            "crop_type_id": 2
          },
          {
            "crop_name": "玉米",
            "crop_type_id": 5
          }
        ]
      }
    }
  },

  // admin-farm - 添加作物
  {
    url: '/add_crop_type',
    type: 'post',
    response: _ => {
      return {
        "message": "Crop type added successfully"
      }
    }
  },

  // admin-farm - 删除作物
  {
    url: '/delete_crop_type',
    type: 'post',
    response: _ => {
      return {
        "message": "Crop type added successfully"
      }
    }
  },
]
