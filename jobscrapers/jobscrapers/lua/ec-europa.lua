function main(splash, args)
  splash.resource_timeout = 60.0
    links = {}
    local url = splash.args.url
    local limit = splash.args.limit
    splash:set_viewport_size(1980, 1020)
    splash.private_mode_enabled = false
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
  	assert(splash:go(url))
    assert(splash:wait(30.0))
    select_country(splash)
    select_button(splash)
    --return splash:png()
  	--select_by50(splash)

    put_keyword(splash)
    push_search(splash)
    while get_links(splash, links) < limit do
      click_next(splash)
      assert(splash:wait(20.0))
    end
    --return splash:png()
    return links
   --return splash:png()
end

function select_country(splash)
  assert(splash:runjs("document.querySelectorAll('ul[class=\"m1-level-1\"] li div')[10].click();"))
  splash:wait(15.0)
end

function select_button(splash)
  assert(splash:runjs("document.querySelectorAll('ul[class=\"drop-down-searchCodes\"] li a')[1].click();"))
  splash:wait(15.0)
end

function select_by50(splash)
  assert(splash:runjs("document.querySelectorAll('select[class=\"nbVacanciesPerPageSelect ng-pristine ng-valid\"] option')[2].selected=true;"))
  splash:wait(15.0)
end

function put_keyword(splash)
  local search = splash.args.search
  print("Search:")
  print(search)
  assert(splash:runjs("document.querySelector('input[ng-model=\"keyword\"]').value=\""..search.."\";"))
  splash:wait(15.0)
end

function push_search(splash)
  assert(splash:runjs("document.querySelector('button[ng-click=\"onSearch()\"]').click();"))
  splash:wait(15.0)
end

function click_next(splash)
  assert(splash:runjs("document.querySelector('div[class=\"float-right pagination-container\"] li a[class=\"page-link next\"]').click();"))
  splash:wait(13.0)
end

function get_links(splash, data)
  local links = splash:select_all('div[ng-repeat="jvSummary in jvs"] label[class="label-XLB-5 not-viewed selectable"] a')
  local i
  for i in ipairs(links) do
    data[#data+1] = links[i].attributes['href']
    print('link '..data[#data]..' was added')
  end
  return #data
end
