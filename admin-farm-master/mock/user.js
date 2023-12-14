
const tokens = {
  admin: {
    token: 'admin-token'
  },
  editor: {
    token: 'editor-token'
  }
}

const users = {
  'admin-token': {
    roles: ['admin'],
    introduction: 'I am a super administrator',
    avatar: 'https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif',
    name: 'Super Admin'
  },
  'editor-token': {
    roles: ['editor'],
    introduction: 'I am an editor',
    avatar: 'https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif',
    name: 'Normal Editor'
  }
}

module.exports = [
  // user login
  {
    url: '/vue-admin-template/user/login',
    type: 'post',
    response: config => {
      const { username } = config.body
      const token = tokens[username]

      // mock error
      if (!token) {
        return {
          code: 60204,
          message: 'Account and password are incorrect.'
        }
      }

      return {
        code: 20000,
        data: token
      }
    }
  },

  // get user info
  {
    url: '/vue-admin-template/user/info\.*',
    type: 'get',
    response: config => {
      const { token } = config.query
      const info = users[token]

      // mock error
      if (!info) {
        return {
          code: 50008,
          message: 'Login failed, unable to get user details.'
        }
      }

      return {
        code: 20000,
        data: info
      }
    }
  },

  // user logout
  {
    url: '/vue-admin-template/user/logout',
    type: 'post',
    response: _ => {
      return {
        code: 20000,
        data: 'success'
      }
    }
  },

  // admin-farm - 用户注册
  {
    url: '/register',
    type: 'post',
    response: _ => {
      return {
        "message": "User registered successfully"
      }
    }
  },

  // admin-farm - 用户登录
  {
    url: '/login',
    type: 'post',
    response: _ => {
      return {
        "email": "admin@123",
        "last_login": "Fri, 08 Dec 2023 20:06:28 GMT",
        "message": "Login successful",
        "organization_name": "Admin Organization",
        "organization_type": "Government",
        "phonenumber": "123123",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxLCJleHAiOjE3MDIxMjM2MjB9.iTGwBBTuposMszhZWxCvo9tjfme-IZb0TjgkjCj4hiY",
        "user_type": 999,
        "username": "admin"
      }
    }
  },

  // admin-farm - 获取用户
  {
    url: '/get_all_users',
    type: 'get',
    response: _ => {
      return [
        {
          "organization_id": 1,
          "password": "admin",
          "phonenumber": "123123",
          "user_id": 1,
          "user_lastonline": "Sun, 10 Dec 2023 22:33:04 GMT",
          "user_online": 1,
          "user_type": 999,
          "useremail": "admin@123",
          "username": "admin"
        },
        {
          "organization_id": 1,
          "password": "govv",
          "phonenumber": "1234561234567",
          "user_id": 5,
          "user_lastonline": "Thu, 07 Dec 2023 12:02:03 GMT",
          "user_online": 1,
          "user_type": 2,
          "useremail": "govv@1234",
          "username": "govv"
        },
        {
          "organization_id": 1,
          "password": "gov",
          "phonenumber": "123456123456",
          "user_id": 4,
          "user_lastonline": "Thu, 07 Dec 2023 12:25:27 GMT",
          "user_online": 1,
          "user_type": 2,
          "useremail": "gov@123",
          "username": "gov"
        },
        {
          "organization_id": 1,
          "password": "govv2",
          "phonenumber": "12345612345678",
          "user_id": 6,
          "user_lastonline": "Fri, 08 Dec 2023 01:12:56 GMT",
          "user_online": 1,
          "user_type": 2,
          "useremail": "govv2@1234",
          "username": "govv2"
        },
        {
          "organization_id": 1,
          "password": "cyan",
          "phonenumber": "18574503525",
          "user_id": 7,
          "user_lastonline": "Fri, 08 Dec 2023 01:40:07 GMT",
          "user_online": 1,
          "user_type": 2,
          "useremail": "cyan@1234",
          "username": "cyan"
        },
        {
          "organization_id": 1,
          "password": "govv3",
          "phonenumber": "123456123456789",
          "user_id": 8,
          "user_lastonline": "Fri, 08 Dec 2023 01:46:28 GMT",
          "user_online": 1,
          "user_type": 2,
          "useremail": "govv3@1234",
          "username": "govv3"
        },
        {
          "organization_id": 1,
          "password": "cyan3",
          "phonenumber": "18500000000",
          "user_id": 9,
          "user_lastonline": "Fri, 08 Dec 2023 15:48:02 GMT",
          "user_online": 1,
          "user_type": 2,
          "useremail": "cyan3@1234",
          "username": "cyan3"
        },
        {
          "organization_id": 1,
          "password": "buy",
          "phonenumber": "1234512345",
          "user_id": 3,
          "user_lastonline": "Fri, 08 Dec 2023 20:44:25 GMT",
          "user_online": 1,
          "user_type": 1,
          "useremail": "buy@123",
          "username": "buy"
        },
        {
          "organization_id": 2,
          "password": "farm",
          "phonenumber": "12341234",
          "user_id": 2,
          "user_lastonline": "Sun, 10 Dec 2023 20:45:35 GMT",
          "user_online": 1,
          "user_type": 0,
          "useremail": "farm@123",
          "username": "farm"
        }
      ]
    }
  },
]