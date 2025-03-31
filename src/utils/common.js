const onError = (node, title, count, error) => {
  const errorMessage = count ? `${title} (${count}): ${error.message}` : `${title}: ${error.message}`
  const statusText = count ? `Error (${count})` : 'Error'

  node.error(errorMessage)
  node.status({
    fill: 'red',
    shape: 'ring',
    text: statusText,
  })
}

const checkLastMessageTime = (node) => {
  if (node.lastMessageTime != null) {
    const timeDiff = new Date().getTime() - node.lastMessageTime
    if (timeDiff > 5000) {
      node.status({ fill: 'yellow', shape: 'ring', text: 'Idle' })
    }
  }
}

module.exports = {
  onError,
  checkLastMessageTime,
}
