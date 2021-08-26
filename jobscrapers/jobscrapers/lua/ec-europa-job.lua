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
    assert(splash:wait(10.0))
    parse_job(splash, data)
    --return splash:png()
    for k, v in pairs(data) do
        print(k..'='..v)
    end

    return data
end


function parse_job(splash, data)
  add_element(splash, data, 'jv-title[jv-title="jv.title"] span', 'position')
  add_element(splash, data, 'h3[class="label-color-5 inline ng-binding ng-scope"]', 'poste')
  add_element(splash, data, 'span[ng-repeat="(countryCode, regionCode) in locationMap"]', 'country')
  add_element(splash, data, 'job-age[class="jobAgeHomePage ng-scope ng-isolate-scope"] span', 'age1')
  add_element(splash, data, 'job-age[class="jobAgeHomePage ng-scope ng-isolate-scope"] span[class="ng-binding"]', 'age2')
  add_element(splash, data, 'div[class="record-description ng-binding"]', 'description')
  add_element_html2(splash, data, 'div[class="margin-top-10"]', 'footer', 2)
  add_element(splash, data, 'div[class="top-bar"] label[class="label-LB-5 ng-binding"]', 'side_title')
  add_element(splash, data, 'details-box-label[title="Expérience:"] label[ng-bind-html="title"]', 'exp_title')
  add_element(splash, data, 'details-box-label[title="Expérience:"] p span', 'exp')
  add_element(splash, data, 'details-box-label[title="Langues:"] label[ng-bind-html="title"]', 'lang_title')
  add_element(splash, data, 'details-box-label[title="Langues:"] p span', 'lang')
  add_element(splash, data, 'details-box-label[title="Permis de conduire:"] label[ng-bind-html="title"]', 'permis_title')
  add_element(splash, data, 'details-box-label[title="Permis de conduire:"] p span', 'permis')
  add_element(splash, data, 'details-box[title="Information sur l\'emploi"] div[class="top-bar"] label[class="label-LB-5 ng-binding"]', 'side_title2')
  add_element(splash, data, 'details-box-label[title="Type de poste:"] label[ng-bind-html="title"]', 'type_title')
  add_element(splash, data, 'details-box-label[title="Type de poste:"] p span', 'type')
  add_element(splash, data, 'details-box-label[title="Type de contrat:"] label[ng-bind-html="title"]', 'contract_type_title')
  add_element(splash, data, 'details-box-label[title="Type de contrat:"] p span', 'contract_type')
  add_element(splash, data, 'details-box[title="Employeur"] div[class="top-bar"] label[class="label-LB-5 ng-binding"]', 'side_title3')
  add_element(splash, data, 'details-box-label[id="employerBox"] p div', 'company')
  add_element(splash, data, 'details-box-label[id="employerBox"] p div[ng-if="jv.employer.website"] span', 'website')
  add_element_html(splash, data, 'div[class="grid_3"]', 'sidebar_html')
end


function add_element(splash, data, selector, name)
  local el = splash:select(selector)
  if el then
    data[name] = el:text()
  end
end

function add_element_html(splash, data, selector, name)
  local el = splash:select(selector)
  if el then
    data[name] = el.innerHTML
  end
end

function add_element_html2(splash, data, selector, name, index)
  local el = splash:select_all(selector)[index]
  if el then
    data[name] = el.innerHTML
  end
end