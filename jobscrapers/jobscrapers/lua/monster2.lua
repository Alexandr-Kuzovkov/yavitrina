function main(splash, args)
  splash.resource_timeout = 60.0
    links = {}
    local url = splash.args.url
    splash:set_viewport_size(1980, 1020)
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
    assert(splash:go(url))
    assert(splash:wait(6.0))
    --return splash:png()
    local el = splash:select('header.title h1.pivot')
    if el then
    	header = el:text()
    else
    	header = '_'
    end
    el = splash:select('header.title h2.figure')
    if el then
    	found = el:text()
    else
    	found = '_'
    end
    local criterie = header .. "---" .. found
    get_jobs_links(splash, links)
    return links, criterie
end


function get_jobs_links(splash, links)
    local limit = splash.args.limit
    local job_links = splash:select_all('div#SearchResults header h2 a')
    for i=1, #job_links do
        print('add link ' .. job_links[i].attributes['href'])
        links[#links+1] = job_links[i].attributes['href']
        if limit and #links >= limit then
            print('Limit is reached...')
            break
        end
    end
end

