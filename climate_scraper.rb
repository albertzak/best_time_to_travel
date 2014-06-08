require 'nokogiri'
require 'cgi'
require 'open-uri'
require 'pstore'
require 'csv'

class Scraper
  BASE_URL = 'http://www.eklima.de/en'

  XPATH_MAIN_TABLE    = '//body/table//table//table//tr[6]/td[2]/table'
  XPATH_CLIMATE_TABLE = XPATH_MAIN_TABLE + '/tr/td[2]/div[3]/table/tr[2]/td/table/tr[position()>1]/td[position()>1]'

  XPATH_COUNTRIES = XPATH_MAIN_TABLE + '/tr/td[1]/div[3]/span/a[2]'
  XPATH_CITIES    = XPATH_MAIN_TABLE + '/tr/td[3]/div[3]/span/a[2]'

  REPLACE = { '++' => 2, '+-' => 1, '--' => 0}

  DEBUG_XPATH_COUNTRIES = '(' + XPATH_MAIN_TABLE + '/tr/td[1]/div[3]/span/a[2])[1]'
  DEBUG_XPATH_CITIES    = '(' + XPATH_MAIN_TABLE + '/tr/td[3]/div[3]/span/a[2])[1]'

  attr_accessor :climates

  def initialize
    @climates = Hash.new
  end

  def perform
    cached_crawl
    build_csv
    puts 'Done.'
  end

  private

  def build_csv
    CSV.open("climates.csv", "wb") do |csv|
      @climates.each_pair do |country, cities|
        cities.each_pair do |city, climate|
          row = []
          row << country
          row << city
          row << climate
          csv << row.flatten
        end
      end
    end
  end

  def cached_crawl
    filename = "cache.pstore"
    @store = PStore.new(filename)

    @store.transaction do
      if @store[:crawl]
        puts "Loading Cache..."
        @climates = @store[:crawl]
        return @store[:crawl]
      else
        puts "Writing Cache..."
        @store[:crawl] = crawl
        @climates = @store[:crawl]
        @store.commit
        return @store[:crawl]
      end
    end
  end

  def crawl
    html = get('/')
    html.xpath(XPATH_COUNTRIES).each do |country|
      add_country(country)
      get(country['href']).xpath(XPATH_CITIES).each do |city|
        add_city(city)
        add_climate(get(city['href']))
      end
    end
    
    @climates
  end

  def add_country(node)
    name = node['title'][33..-1].gsub(',', ' ')

    @last_country = name

    puts name

    @climates[name] = Hash.new
  end

  def add_city(node)
    name = node['title'][(41 + @last_country.length)..-1].gsub(',', '')
    
    @last_city = name

    @climates[@last_country][name] = Hash.new

    puts [@last_country, name].join(' - ')
  end

  def add_climate(node)
    ratings = node.xpath(XPATH_CLIMATE_TABLE)
    ratings = ratings.text.scan(/.{2}/).map { |s| REPLACE[s] }
    total = ratings.count
    rows = total / 12

    ratings = (0..11).map do |month|
      (0..rows-1).map do |row|
        ratings[row*12+month]
      end.reduce(:+)
    end

    ratings.map! { |r| r.to_f / (rows*2) }

    @climates[@last_country][@last_city] = ratings
  end

  def get(path)
    path = URI.escape(path[/[^\#]+/])
    url  = BASE_URL
    url  = [url, path].join('/')

    puts url
    
    open(URI.parse(url)) do |f|
      html = Nokogiri::HTML(f)
      html
    end
  end

end

Scraper.new.perform
