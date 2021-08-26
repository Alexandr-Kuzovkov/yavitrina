function main(splash)
  	splash.resource_timeout = 60.0
    data = {}
    local url = splash.args.url
    local elem
    assert(splash:go(url))
    assert(splash:wait(5.0))
    splash:set_viewport_full()
  	splash:wait(5.0)

    elem = splash:select('div#jobViewTitle')
    if elem ~= nil then
        data['title'] = elem:text()
    else
        data['title'] = nil
    end

    elem = splash:select('div.job-full-description__content')
    if elem ~= nil then
        data['description'] = elem:text()
    else
        data['description'] = nil
    end

    elem = splash:select('div#jobViewCreatedAt')
    if elem ~= nil then
        data['posted_at'] = elem:text()
    else
        data['posted_at'] = nil
    end

    elem = splash:select('div#jobViewCategory')
    if elem ~= nil then
        data['category'] = elem:text()
    else
        data['category'] = nil
    end

    elem = splash:select('div#jobViewLocation')
    if elem ~= nil then
        data['location'] = elem:text()
    else
        data['location'] = nil
    end

    elem = splash:select('div#jobViewType')
    if elem ~= nil then
        data['job_type'] = elem:text()
    else
        data['job_type'] = nil
    end

    elem = splash:select('div.sidebar-company__link a')
    if elem ~= nil then
        data['profile_link'] = elem.attributes['href']
    else
        data['profile_link'] = nil
    end

    elem = splash:select('div.sidebar-company__logo img')
    if elem ~= nil then
        data['logo_url'] = elem.attributes['src']
    else
        data['logo_url'] = nil
    end
    return data
end


