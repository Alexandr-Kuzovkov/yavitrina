function main(splash, args)
    splash.resource_timeout = 60.0
    links = {}
    local url = splash.args.url
    local limit = splash.args.limit
    splash:set_viewport_size(1980, 1020)
    splash.private_mode_enabled = false
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    splash.http2_enabled = true
    splash.indexeddb_enabled = true
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
  	ok, reason = splash:go(url)
    splash:wait(10.0)
    chart = splash:select('div[class="related-products"]')
    return {
        html = splash:html(),
        png = splash:png(),
        har = splash:har(),
        chart = chart:png()
      }

end
