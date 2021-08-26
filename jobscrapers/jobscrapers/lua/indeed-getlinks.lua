function main(splash, args)
  splash.resource_timeout = 60.0
    links = {}
    local url = splash.args.url
    splash:set_viewport_size(1980, 1020)
    splash.private_mode_enabled = false
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
  	assert(splash:go(url))
    assert(splash:wait(3.0))
    print(get_amount_cvs(splash))
    parse_links(splash, links)
    local limit = splash.args.limit
    local page_number = 1
    print('page_number:' .. tostring(page_number))
  	while next_page(splash) and not error_occured(splash) do
        parse_links(splash, links)
        page_number = page_number + 1
        print('page_number:' .. tostring(page_number))
        if limit and #links >= limit then
            break
        end
    end
    return links
end

function parse_links(splash, links)
    local items = splash:select_all('div[class="rezemp-ResumeSearchCard-contents"] div span a')
    print(#items)
    local j
  	for j in ipairs(items) do
        links[#links+1] = items[j].attributes['href']
        print(items[j].attributes['href'])
    end
end

function next_page(splash)
    local next_link = splash:select('span[class="icl-TextLink icl-TextLink--primary rezemp-pagination-nextbutton"]')
    print('Next link: ' .. tostring(next_link))
    if next_link then
        print(next_link:mouse_click())
        splash:wait(2.0)
        return true
    else
        return false
    end
end

function error_occured(splash)
    local span = splash:select('span[class="icl-Alert-headline"]')
    if span then
        local text = span:text()
        print('Message: ' .. text)
        if text == 'Error' then
            return true
        end
    end
    return false
end

function get_amount_cvs(splash)
    local div = splash:select('div[class="icl-u-textColor--tertiary"]')
    if div then
        return div:text()
    end
    return 'No found...'
end


