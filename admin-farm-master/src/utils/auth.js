import Cookies from 'js-cookie'

const TokenKey = 'vue_admin_template_token'

export function getToken() {
  return Cookies.get(TokenKey)
}

export function setToken(token) {
  return Cookies.set(TokenKey, token)
}

export function removeToken() {
  return Cookies.remove(TokenKey)
}



const FarmTokenKey = 'admin_farm_token'
export function getFarmToken() {
  return Cookies.get(FarmTokenKey)
}

export function setFarmToken(token) {
  return Cookies.set(FarmTokenKey, token)
}

export function removeFarmToken() {
  return Cookies.remove(FarmTokenKey)
}
