function main(splash, args)
  splash.resource_timeout = 60.0
    data = {}
    local url = splash.args.url
    splash:set_viewport_size(1980, 1020)
    splash.private_mode_enabled = false
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
  	assert(splash:go(url))
    assert(splash:wait(3.0))

    local cvbody = splash:select('div[class="rezemp-ResumeDisplay rezemp-ResumeDisplay-fixed"]')
    if cvbody then
        data['png'] = cvbody:png()
        data['html'] = cvbody.innerHTML
    else
        data['png'] = splash:png()
        data['html'] = splash:html()
    end
    return data['png'], data['html']
end






