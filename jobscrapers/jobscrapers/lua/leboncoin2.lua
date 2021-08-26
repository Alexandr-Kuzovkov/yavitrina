function main(splash, args)
    splash.resource_timeout = 60.0
    data = {}
    local url = splash.args.url
    splash:set_viewport_size(1980, 1020)
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
    assert(splash:go(url))
    assert(splash:wait(3.0))
    click_to_link(splash)
    splash:wait(3.0)
    return splash.html()
end

function click_to_link(splash)
    local link = splash:select('span[class="TextLink-15wnQ"]')
    if link then
        print('link was found')
        assert(splash:runjs("document.getElementsByClassName('TextLink-15wnQ')[0].click();"))
        splash:wait(2.0)
    end
end
