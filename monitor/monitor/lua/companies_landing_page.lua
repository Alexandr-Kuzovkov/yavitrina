function main(splash)
  	local data = {}
  	local count = 0
  	data = rendering(splash)
  	while #data['jobs'] == 0 and count < 7 do
        count  = count+1
        data = rendering(splash)
    end
    return data
end


function rendering(splash)
    splash.resource_timeout = 60.0
    data = {}
    local url = splash.args.url
    local elem
    assert(splash:go(url))
    assert(splash:wait(5.0))
    splash:set_viewport_full()
  	splash:wait(5.0)

    elem = splash:select('div#companyName')
    if elem ~= nil then
        data['name'] = elem:text()
    else
        data['name'] = nil
    end

    elem = splash:select('div.text-block')
    if elem ~= nil then
        data['description'] = elem:text()
    else
        data['description'] = nil
    end

    elem = splash:select('div.sidebar-company__logo img')
    if elem ~= nil then
        data['logo'] = elem.attributes['src']
    else
        data['logo'] = nil
    end

    elem = splash:select('div.sidebar-company__link a')
    if elem ~= nil then
        data['url'] = elem.attributes['href']
    else
        data['url'] = nil
    end

    data['jobs'] = get_jobs(splash)
    print(#data['jobs'])
    data['social_links'] = get_social_links(splash)
    return data
end

function get_jobs(splash)
    local jobs = {}
    local job = {}
    local job_divs = splash:select_all('div.job')
    for i in ipairs(job_divs) do
        job = {}
        job['url'] = job_divs[i].node.firstChild.firstChild.firstChild.attributes['href']
        jobs[#jobs + 1] = job
    end
    return jobs
end

function get_social_links(splash)
    social_links = {}
    local links = splash:select_all('div.sidebar-company__socials a')
    for i in ipairs(links) do
        social_links[#social_links+1] = links[i].node.attributes['href']
    end
    return social_links
end


