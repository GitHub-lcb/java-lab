export function describeGatewayError(error) {
  const message = error && typeof error.message === 'string' ? error.message : String(error || '未知错误');
  const name = error?.name || '';
  if (error instanceof TypeError || /failed to fetch|load failed|networkerror|connection refused|ECONNREFUSED/i.test(message)) {
    return { kind: 'offline', title: '无法连接本机 Java 网关', hint: '请先在项目根目录执行 npm run backend 启动网关（首次先 npm ci；需 JDK 8+ 的 javac），就绪后重新执行。', detail: message };
  }
  if (name === 'TimeoutError' || /timed? out|abort/i.test(message)) {
    return { kind: 'timeout', title: '请求超时，网关未及时响应', hint: '执行可能已在网关侧生效：若刚提交过写入类命令，请先查询实际状态，不要直接重复执行。', detail: message };
  }
  if (/gateway request failed|jvm probe failed|^http \d/i.test(message)) {
    return { kind: 'gateway', title: '网关返回异常', hint: '请点击「重新检测」查看网关状态；问题持续时可查看网关进程日志。', detail: message };
  }
  return { kind: 'other', title: message, hint: '', detail: '' };
}
