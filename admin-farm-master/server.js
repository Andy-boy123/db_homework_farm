const axios = require('axios');

console.log(axios)

async function sendRequest() {
    const url = 'http://api3.andylive.cn/register';
    const data = {
        'username': 'cyan',
        'phonenumber': '18574503525',
        'organization': 1,
        'useremail': 'cyan@1234',
        'password': 'cyan',
        'user_type': 2
    };
    const config = {
        headers: {
            // 如有需要, 在这里设置请求头部
        }
    };

    try {
        const response = await axios.post(url, data, config);
        console.log('Response:', response.data);
    } catch (error) {
        console.error('Error:', error);
    }
}

sendRequest();
