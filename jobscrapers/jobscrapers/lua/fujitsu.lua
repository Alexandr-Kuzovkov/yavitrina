function main(splash)
  	splash.resource_timeout = 60.0
    links = {}
    local url = splash.args.url
    assert(splash:go(url))
    assert(splash:wait(0.5))
    splash:set_viewport_full()
  	splash:wait(2.0)
    parse_links(splash, links)
    local page_number = 1
    print('page_number:' .. tostring(page_number))
  	while next_page(splash) and page_number < 211 do
        parse_links(splash, links)
        page_number = page_number + 1
        print('page_number:' .. tostring(page_number))
    end
    return links
end

function parse_links(splash, links)
    local link = {}
    local item = splash:select('a.jobTitle')
    local count = 0
    while item:exists() == false and count < 20 do
        splash:wait(1.0)
        count = count + 1
    end
    local items = splash:select_all('a.jobTitle')
    local j
  	for j in ipairs(items) do
        links[#links+1] = items[j].attributes['href']
        print(items[j].attributes['href'])
    end
end

function next_page(splash)
    local next_link = splash:select('a[title="Next Page"]')
    if next_link then
        next_link:mouse_click()
        splash:wait(2.0)
        return true
    else
        return false
    end
end

