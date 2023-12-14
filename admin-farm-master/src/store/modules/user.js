import { login, loginFarm, logoutFarm, getInfo } from '@/api/user'
import {
  getToken, setToken, removeToken,
  getFarmToken, setFarmToken, removeFarmToken
} from '@/utils/auth'
import { constantRoutes, resetRouter } from '@/router'

const getDefaultState = () => {
  return {
    token: getToken(),
    name: '',
    avatar: '',

    farmToken: getFarmToken(),
    userName: '',
    userInfo: {},
    roles: []
  }
}

const state = getDefaultState()

const mutations = {
  RESET_STATE: (state) => {
    Object.assign(state, getDefaultState())
  },
  SET_TOKEN: (state, token) => {
    state.token = token
  },
  SET_NAME: (state, name) => {
    state.name = name
  },
  SET_AVATAR: (state, avatar) => {
    state.avatar = avatar
  },

  SET_FARM_TOKEN: (state, token) => {
    state.farmToken = token
  },
  SET_FARM_NAME: (state, name) => {
    state.userName = name
  },
  SET_USER_INFO: (state, _userinfo) => {
    console.log('_userinfo', _userinfo)
    state.userInfo = _userinfo
  },
  SET_ROLES: (state, roles) => {
    state.roles = roles
  }
}

const actions = {
  // user login
  login({ commit }, userInfo) {
    const { username, password } = userInfo
    return new Promise((resolve, reject) => {
      login({ username: username.trim(), password: password }).then(response => {
        const { data } = response
        commit('SET_TOKEN', data.token)
        setToken(data.token)
        resolve()
      }).catch(error => {
        reject(error)
      })
    })
  },

  loginFarm({ commit }, userInfo) {
    const { username, password } = userInfo
    return new Promise((resolve, reject) => {
      loginFarm({ login: username.trim(), password: password }).then(response => {
        const { token, username } = response

        commit('SET_FARM_TOKEN', token)
        setFarmToken(token)

        commit('SET_FARM_NAME', username)
        localStorage.setItem('_username', username)

        commit('SET_USER_INFO', response)
        localStorage.setItem('_userinfo', JSON.stringify(response, null, 4))

        resolve()

      }).catch(error => {
        reject(error)
      })
    })
  },

  // get user info
  _getInfo({ commit, state }) {
    return new Promise((resolve, reject) => {
      getInfo(state.token).then(response => {
        const { data } = response

        if (!data) {
          return reject('Verification failed, please Login again.')
        }

        const { name, avatar } = data

        commit('SET_NAME', name)
        commit('SET_AVATAR', avatar)
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  getInfo({ commit, state }) {
    return new Promise((resolve, reject) => {
      if (state.farmToken) {
        const _username = localStorage.getItem('_username')
        commit('SET_FARM_NAME', _username)
        
        const _userinfo = localStorage.getItem('_userinfo')
        const userinfo = JSON.parse(_userinfo)
        commit('SET_USER_INFO', userinfo)

        // 0 农户	1 粮企	2 政府用户	999 管理员
        commit('SET_ROLES', [userinfo.user_type])

        resolve([userinfo.user_type])
      } else {
        reject()
      }

    })
  },

  // user logout
  logout({ commit, state }) {
    return new Promise((resolve, reject) => {
      logoutFarm(state.token).then(() => {
        localStorage.removeItem('_username')
        localStorage.removeItem('_userinfo')
        // removeToken() // must remove  token  first
        removeFarmToken()
        resetRouter()
        commit('RESET_STATE')
        resolve()
      }).catch(error => {
        reject(error)
      })
    })
  },

  // remove token
  resetToken({ commit }) {
    return new Promise(resolve => {
      removeToken() // must remove  token  first
      commit('RESET_STATE')
      resolve()
    })
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions
}

