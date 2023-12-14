import request from "@/utils/request";

// admin-farm - 获取所有记录（政府，管理员） done
export function getAllSales() {
  return request({
    url: "/get_all_sales",
    method: "get",
  });
}
// admin-farm - 获取市场信息（粮企）done
export function getViewSalesMarket() {
  return request({
    url: "/view_sales_market",
    method: "get",
  });
}
// admin-farm - 获取销售记录（农户） done
export function getPersonalSales() {
  return request({
    url: "/get_personal_sales",
    method: "get",
  });
}

// admin-farm - 添加销售记录（农户）  done
export function addSale(data) {
  return request({
    url: "/add_sale",
    method: "post",
    data,
  });
}

// admin-farm - 删除销售记录 （农户）  sale_time为null可以删，非空不可以删除
export function deleteSale(data) {
  return request({
    url: "/delete_sale",
    method: "post",
    data,
  });
}

// admin-farm - 添加购买记录 （ 粮企）
export function addPurchase(data) {
  return request({
    url: "/add_purchase",
    method: "post",
    data,
  });
}
