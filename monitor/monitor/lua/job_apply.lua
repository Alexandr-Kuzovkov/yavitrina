function main(splash)
  	splash.resource_timeout = 60.0
    data = {}
    local url = splash.args.url
    assert(splash:go(url))
    assert(splash:wait(1.5))
    splash:set_viewport_full()
  	splash:wait(10.0)

    local button = splash:select('button.button-apply')
    if button == nil then
        data['error'] = 'Button "Apply" not found'
        return data
    end

  	if not button:visible() then
    	  data['error'] = 'Button "Apply" not visible'
        return data
    end

    local bounds = button:bounds()
    assert(button:mouse_click{x=bounds.width/2, y=bounds.height/2})

    local name = 'Scrapy Monitor'
    local email = 'scrapy' .. tostring(math.random(1,1000000)) .. '@mail.com'
    local phohe = tostring(math.random(100000000,900000000))

    local form = splash:select('div.modal-content form')
    if form == nil then
        data['error'] = 'Form not found'
        return data
    end

  	local fieldName = splash:select('input#nameField')
  	if fieldName == nil then
    		data['error'] = 'Field name not found'
        return data
    end
  	fieldName.node.value = name

  	local fieldEmail = splash:select('input#emailField')
  	if fieldEmail == nil then
    		data['error'] = 'Field email not found'
        return data
    end
  	fieldEmail.node.value = email

  	local fieldPhone = splash:select('input#phoneField')
  	if fieldPhone == nil then
    		data['error'] = 'Field phone not found'
        return data
    end
  	fieldPhone.node.value = phone

  	local fileResume = splash:select('input#resumeField')
  	if fileResume == nil then
    		data['error'] = 'Field fileResume not found'
        return data
    end
  	fileResume.node.value = '/home/user1/Downloads/download.pdf'

  	local sendBtn = splash:select('input#send-button')
  	if sendBtn == nil then
    		data['error'] = 'Field sendBtn not found'
        return data
    end
  	sendBtn.node.enabled = enabled
  	splash:wait(1.0)

  	data['result'] = 'success'
  	return data
end


