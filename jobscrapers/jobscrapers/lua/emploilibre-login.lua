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
    local email = splash.args.email
    local password = splash.args.password
    local search = splash.args.search
    print('Trying login...')
    local form = splash:select('form[action="modules/connexion"]')
    local values = assert(form:form_values())
    values.nex_identifiant = email
    values.nex_mot_de_passe = password
    assert(form:fill(values))
    form:submit()
    assert(splash:wait(3.0))
    assert(splash:go('https://www.emploilibre.fr/compte/cvtheque-recherche'))
    assert(splash:wait(2.0))
    local form2 = splash:select('form[id="recherche_cv"]')
    local values2 = assert(form2:form_values())
    print('Login success')
    assert(splash:wait(600.0))
    return 'Done'
end

