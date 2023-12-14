module.exports = [

  // admin-farm 获取地块信息视图
  {
    url: '/get_massifs',
    type: 'get',
    response: _ => {
      return {
        "massifs": [
          {
            "crop_type": "小麦",
            "harvest_date": "2023-09-30",
            "id": 2,
            "owner": "admin",
            "planting_date": "2023-05-01",
            "user_id": 1
          }
        ]
      }
    }
  },

  // admin-farm - 添加地块信息
  {
    url: '/add_massif',
    type: 'post',
    response: _ => {
      return {
        "message": "Crop type added successfully"
      }
    }
  },

  // admin-farm - 修改地块信息
  {
    url: '/update_massif',
    type: 'post',
    response: _ => {
      return {
        "message": "Crop type added successfully"
      }
    }
  },

  
  // admin-farm - 删除地块信息
  {
    url: '/delete_massif',
    type: 'post',
    response: _ => {
      return {
        "message": "Crop type added successfully"
      }
    }
  },
]
