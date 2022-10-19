import scrapy
import pandas as pd
df = pd.read_csv('F:\Web Scraping\Golabal\manta.csv')
base_url = 'https://www.yelp.com/search?find_desc=Restaurants&find_loc={}%2C+{}&start=0'
# https://www.yelp.com/search?find_desc=Restaurants&find_loc=Dallas%2C+TX
class YelSpider(scrapy.Spider):
    name = 'yel'
    def start_requests(self):
        cities = df['CITY'].values.tolist()
        states = df['STATES'].values.tolist()
        counter = 0
        for city in cities:
            state = states[counter]
            yield scrapy.Request(base_url.format(city,state), cb_kwargs={'city':city,'state':state})
            counter = counter+1
    def parse(self, response, city,state):
        # print(response.url)          
        pages1 = response.css(" div.text-align--center__09f24__fYBGO span::text").get()
        pages2 = pages1.split('of ')
        total_pages = pages2[1]
        # print(total_pages)      
        current_page =response.css(".pagination-link--current__09f24__vBjKh::text").get()  
        # print(current_page)
        url = response.url   
        start = 0     
        if total_pages and current_page:
            if int(current_page) ==1:
                for i in range(1, int(total_pages)): 
                    start = start+10
                    min = 'start='+str(start-10)
                    max = 'start='+str(start)
                    url = url.replace(min,max)  
                    # print(url)                          
                    yield response.follow(url, cb_kwargs={'city':city,'state':state})       

        links = response.css("a.css-1m051bw::attr(href)")
        for link in links:           
            yield response.follow("https://www.yelp.com"+link.get(),  callback=self.parse_item, cb_kwargs={'city':city,'state':state})  
           
    def parse_item(self, response, city, state): 
        print(".................")        
        website = response.css("p.css-1p9ibgf a.css-1um3nx::text").get()
        print(website)        
        name = response.css("h1.css-1se8maq::text").get()
        print(name)     
        location = response.css(" p.css-qyp8bo::text").get()
        print(location)
        phone = response.xpath("//div[2]/div/div[1]/p[2]/text()").get()
        print(phone)
        about = response.xpath("//p[@class=' css-1evauet']/span/span/span/text()").getall()
        print(about)   

        yield{   
            'name' : name,  
            'phone' : phone,
            'categories' : about,
            'location' : location,  
            'city' : city,
            'state_name' : state,
            'website' : website,
            'yellowpages_url' : response.url,              
                
        }
