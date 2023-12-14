import request from "@/utils/request";

// admin-farm - 获取地块信息视图
export function getMassifs() {
  return request({
    url: "/get_massifs",
    method: "get",
  });
}

// admin-farm - 添加地块信息
export function addMassifs(data) {
  return request({
    url: "/add_massif",
    method: "post",
    data,
  });
}

// admin-farm - 修改地块信息
export function updateMassifs(data) {
  return request({
    url: "/update_massif",
    method: "post",
    data,
  });
}

// admin-farm - 删除地块信息
export function deleteMassifs(data) {
  return request({
    url: "/delete_massif",
    method: "post",
    data,
  });
}
