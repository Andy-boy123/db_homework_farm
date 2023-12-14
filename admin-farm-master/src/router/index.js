import Vue from 'vue'
import Router from 'vue-router'

Vue.use(Router)

/* Layout */
import Layout from '@/layout'

/**
 * Note: sub-menu only appear when route children.length >= 1
 * Detail see: https://panjiachen.github.io/vue-element-admin-site/guide/essentials/router-and-nav.html
 *
 * hidden: true                   if set true, item will not show in the sidebar(default is false)
 * alwaysShow: true               if set true, will always show the root menu
 *                                if not set alwaysShow, when item has more than one children route,
 *                                it will becomes nested mode, otherwise not show the root menu
 * redirect: noRedirect           if set noRedirect will no redirect in the breadcrumb
 * name:'router-name'             the name is used by <keep-alive> (must set!!!)
 * meta : {
    roles: ['admin','editor']    control the page roles (you can set multiple roles)
    title: 'title'               the name show in sidebar and breadcrumb (recommend set)
    icon: 'svg-name'/'el-icon-x' the icon show in the sidebar
    breadcrumb: false            if set false, the item will hidden in breadcrumb(default is true)
    activeMenu: '/example/list'  if set path, the sidebar will highlight the path you set
  }
 */

/**
 * constantRoutes
 * a base page that does not have permission requirements
 * all roles can be accessed
 * 权限组 0是农户，1是粮企，2是政府用户，999管理员
 * 首页，作物表，个人信息表都可以访问
 */
export const constantRoutes = [
  {
    path: '/login',
    component: () => import('@/views/login/index'),
    hidden: true
  },

  {
    path: '/404',
    component: () => import('@/views/404'),
    hidden: true
  },

  {
    path: '/',
    component: Layout,
    redirect: '/profile/index',
  },
  
  // {
  //   path: '/crops',
  //   component: Layout,
  //   children: [
  //     {
  //       path: 'index',
  //       name: 'CropsIndex',
  //       component: () => import('@/views/crops/index'),
  //       meta: {  roles: ['admin2'], title: '作物', icon: 'list' }
  //     }
  //   ]
  // },

  {
    path: '/profile',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'ProfileIndex',
        component: () => import('@/views/profile/index'),
        meta: { title: '个人信息', icon: 'list' }
      }
    ]
  },

  // {
  //   path: '/example',
  //   component: Layout,
  //   redirect: '/example/table',
  //   name: 'Example',
  //   meta: { title: 'Example', icon: 'el-icon-s-help' },
  //   children: [
  //     {
  //       path: 'table',
  //       name: 'Table',
  //       component: () => import('@/views/table/index'),
  //       meta: { title: 'Table', icon: 'table' }
  //     },
  //     {
  //       path: 'tree',
  //       name: 'Tree',
  //       component: () => import('@/views/tree/index'),
  //       meta: { title: 'Tree', icon: 'tree' }
  //     }
  //   ]
  // },
]

export const asyncRoutes = [
  
  {
    path: '/massifs',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'MassifsIndex',
        component: () => import('@/views/massifs/index'),
        meta: { roles: [999, 0, 2], title: '地块管理', icon: 'list' }
      }
    ]
  },
  {
    path: '/harvests',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'HarvestsIndex',
        component: () => import('@/views/harvests/index'),
        meta: { roles: [999, 2, 0], title: '收获管理', icon: 'list' }
      }
    ]
  },
  {
    path: '/irrigations',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'IrrigationsIndex',
        component: () => import('@/views/irrigations/index'),
        meta: { roles: [999, 2, 0], title: '灌溉管理', icon: 'list' }
      }
    ]
  },
  {
    path: '/sales',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'SalesIndex',
        component: () => import('@/views/sales/index'),
        meta: { roles: [0, 1, 2, 999], title: '销售记录', icon: 'list' }
      }
    ]
  },
  {
    path: '/crops',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'CropsIndex',
        component: () => import('@/views/crops/index'),
        meta: { roles: [0, 1, 2, 999], title: '作物管理', icon: 'list' }
      }
    ]
  },
  {
    path: '/users',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'UsersIndex',
        component: () => import('@/views/users/index'),
        meta: { roles: [999], title: '用户管理', icon: 'list' }
      }
    ]
  },
  {
    path: '/organizations',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'OrganizationsIndex',
        component: () => import('@/views/organizations/index'),
        meta: { roles: [999, 2], title: '组织管理', icon: 'list' }
      }
    ]
  },

  // 404 page must be placed at the end !!!
  { path: '*', redirect: '/404', hidden: true }
]

const createRouter = () =>
  new Router({
    // mode: 'history', // require service support
    scrollBehavior: () => ({ y: 0 }),
    routes: constantRoutes
  })

const router = createRouter()

// Detail see: https://github.com/vuejs/vue-router/issues/1234#issuecomment-357941465
export function resetRouter() {
  const newRouter = createRouter()
  router.matcher = newRouter.matcher // reset router
}

export default router
