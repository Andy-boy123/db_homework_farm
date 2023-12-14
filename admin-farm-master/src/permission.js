import router from './router'
import store from './store'
import { Message } from 'element-ui'
import NProgress from 'nprogress' // progress bar
import 'nprogress/nprogress.css' // progress bar style
import { getToken, getFarmToken } from '@/utils/auth' // get token from cookie
import getPageTitle from '@/utils/get-page-title'

NProgress.configure({ showSpinner: false }) // NProgress Configuration

const whiteList = ['/login'] // no redirect whitelist

router.beforeEach(async(to, from, next) => {
  // start progress bar
  NProgress.start()

  // set page title
  document.title = getPageTitle(to.meta.title)

  // determine whether the user has logged in
  // const hasToken = getToken()
  const hasToken = getFarmToken()


  console.group('hasToken')
  console.log(hasToken)
  console.groupEnd()

  if (hasToken) {
    if (to.path === '/login') {
      // if is logged in, redirect to the home page
      next({ path: '/' })
      NProgress.done()
    } 
    // else {
    //   // const hasGetUserInfo = store.getters.name
    //   const hasGetUserInfo = store.getters.userName
    //   if (hasGetUserInfo) {
    //     next()
    //   } else {
    //     try {
    //       // get user info
    //       await store.dispatch('user/getInfo')

    //       next()
    //     } catch (error) {
    //       // remove token and go to login page to re-login
    //       await store.dispatch('user/resetToken')
    //       Message.error(error || 'Has Error')
    //       next(`/login?redirect=${to.path}`)
    //       NProgress.done()
    //     }
    //   }
    // }
    else{
    // 确定用户是否通过getInfo获得了权限角色
    const hasRoles = store.getters.roles && store.getters.roles.length > 0 //这里指的是src/store/getters.js的roles
    // console.log(hasRoles)
    // console.log(store.getters.roles)

    //判断是否已经有roles了
    if (hasRoles) {
      next(); //当有用户权限的时候，说明所有可访问路由已生成 如访问没权限的全面会自动进入404页面
    } else {
      try {
          const roles = await store.dispatch('user/getInfo') //第一步
          const accessRoutes = await store.dispatch('permission/generateRoutes', roles)
          router.addRoutes(accessRoutes)
          next({
            ...to,
            replace: true
          })

        } catch (error) {
          // remove token and go to login page to re-login
          await store.dispatch('user/resetToken')
          Message.error(error || 'Has Error')
          next(`/login?redirect=${to.path}`)
          NProgress.done()
        }
      }
    }

  } else {
    /* has no token*/

    if (whiteList.indexOf(to.path) !== -1) {
      // in the free login whitelist, go directly
      next()
    } else {
      // other pages that do not have permission to access are redirected to the login page.
      next(`/login?redirect=${to.path}`)
      NProgress.done()
    }
  }
})

router.afterEach(() => {
  // finish progress bar
  NProgress.done()
})
