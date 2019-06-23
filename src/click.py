import requests

url = "https://sp0.baidu.com/9q9JcDHa2gU2pMbgoY3K/adrc.php?t=060X06c00fDg3Kb0pZ-b0nL5hf0JYs4I000005-YQNC00000TIUa3c.THv3LeStdoeR80K85yF9pywdpAqVuNqsusK15yRLnWfYn1whnj0zuWbknvc0IHY4wDDLfYnvfbf3njKarHndnYczPDF7nDDYn1RzwRn1f6K95gTqFhdWpyfqn1cdnHTLrH6snausThqbpyfqnHf0uHdCIZwsT1CEQLILIz44ULNlXi4WUBqETAN8RA7zIA4-TMnEXyqdXMbEpy4bug68pZwVUjqduMnqnW0krN-Bmy-bIfKWThnqPHDvPjn&tpl=tpl_11760_20143_16228&l=1512928505&attach=location%3D%26linkName%3D%25E6%2596%2587%25E6%259C%25AC_1-%25E9%2593%25BE%25E6%258E%25A5%25E6%2596%2587%25E6%259C%25AC%26linkText%3D%25E9%25AB%2598%25E8%2580%2583%25E5%25A1%25AB%25E5%25BF%2597%25E6%2584%25BF%25E5%25B0%25B1%25E6%2589%25BE%25E4%25BC%2598%25E5%25BF%2597%25E6%2584%25BF%26xp%3Did(%2522m3251779800_canvas%2522)%252FDIV%255B1%255D%252FDIV%255B1%255D%252FDIV%255B1%255D%252FA%255B1%255D%26linkType%3D%26checksum%3D49%26cid%3D299%26cversion%3D36987%26cpos%3D1-1&wd=%E9%AB%98%E8%80%83%E5%BF%97%E6%84%BF&issp=1&f=8&ie=utf-8&rqlang=cn&tn=baiduhome_pg&inputT=1420&oq=%25E9%25AB%2598%25E8%2580%2583"
def get_proxy():
    return requests.get("http://118.24.52.95:5010/get/").content

# your spider code
def getHtml():
    # ....
    retry_count = 2
    proxy = get_proxy()
    while retry_count > 0:
        try:
            html = requests.get(url, proxies={"http": "http://{}".format(proxy)})
            # 使用代理访问
            return html.status_code
        except Exception:
            retry_count -= 1
    return None

if __name__ == '__main__':
    while True:
        print(getHtml())

