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
    assert(splash:wait(3.0))
    --select_country(splash)
    --assert(splash:wait(3.0))
    criterie = 'Tout afficher'
    get_jobs_links(splash, links)
    crawl_pages(splash, links)
   return links, criterie
end


function select_country(splash)
    local select =  splash:select('select#ctl00_ctl00_moteurRapideOffre_ctl01_OfferCriteria_Location_GeographicalAreaCollection')
    if select:exists() == true then
        print('select was found')
        select:mouse_click()
        splash:wait(1.0)
        local country = splash.args.country
        --Europe-->France 32 is index in select -> option
        for i=1, country  do
            print(select:send_keys("<Down>"))
            splash:wait(0.5)
        end
        print(select:send_keys("<Return>"))
        splash:wait(1.0)
        print(select:field_value())
        local button = splash:select('input#ctl00_ctl00_moteurRapideOffre_BT_recherche')
        if button:exists() == true then
            button:mouse_click()
            splash:wait(1.0)
            return true
        end
    end
    return false
end

function get_jobs_links(splash, links)
    local  criterie = 'Tout afficher'
    print('FILTER: ' .. criterie)
    local job_links = splash:select_all('a.ts-offer-card__title-link')
    for i=1, #job_links do
        print('add link ' .. job_links[i].attributes['href'])
        links[#links+1] = job_links[i].attributes['href']
    end
end

function crawl_pages(splash, links)
    local page_links = splash:select_all('ul[class="ts-ol-pagination-list PrecSuivant"] li a')
    local res = {}
    local hash = {}
    --remove duplicates
    for _,v in ipairs(page_links) do
        if (not hash[v.attributes['href']]) then
            res[#res+1] = v.attributes['href']
            hash[v.attributes['href']] = true
        end
    end

    for i=1, #res do
        print(i)
        print('Crawle page '.. res[i])
    		local page_url = res[i]
        print('before go')
        assert(splash:go(page_url))
        splash:wait(3.0)
        print('after go')
        get_jobs_links(splash, links)
        local limit = splash.args.limit
        if limit and #links >= limit then
            break
        end
    end
end