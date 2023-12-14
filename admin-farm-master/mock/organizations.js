
module.exports = [

  // admin-farm - 获取用人单位
  {
    url: '/get_organizations',
    type: 'get',
    response: config => {
      return [
        {
          "additional_info": "Organization for administrative users",
          "organization_id": 1,
          "organization_name": "Admin Organization",
          "organization_type": "Government"
        },
        {
          "additional_info": null,
          "organization_id": 2,
          "organization_name": "a县人民政府",
          "organization_type": "政府机构"
        }
      ]
    }
  },

  // admin-farm - 添加用人单位
  {
    url: '/add_organization',
    type: 'post',
    response: _ => {
      return {
        "message": "successfully"
      }
    }
  },

  // admin-farm - 删除用人单位
  {
    url: '/delete_organization',
    type: 'post',
    response: _ => {
      return {
        "message": "successfully"
      }
    }
  },
]
