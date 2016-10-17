# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 02:07:11 2016

@author: Евгений
"""

# import pandas as pd
# z = pd.read_csv("_all2013.csv", sep = ";")
# d = z[['inn','okved','title','name','_2110','_1410']]
# s = d[d['_1410']>0].sort_values('okved')


MORE_INN = [5640005415,7726311464,7717665234,
            1434045743, 7701897590, 2319008223,
            2310106385,5406409682,4003034171,
            2723127073,276135911,5261086749,
            2009000023, 6234020740]
            
NOT_FOUND = [5032178356, 5010032360, 262016287]



def get_inn(s):
   return [(i, x) for i, x in zip(z['inn'], z['name']) 
            if s.lower() in x.lower()]

def get_inn2(s, t):
   return [(i, x) for i, x in zip(z['inn'], z['name']) 
            if s.lower() in x.lower() and t.lower() in x.lower()] 

LOOKUP = ["ВКМ-Сталь", "Адмиралтейские верфи"
          ,"Уралвагонзавод", "Славобласть"]
for l in LOOKUP:
    print(get_inn(l))

#ix = [x in MORE_INN for x in z['inn']]
#z[ix].to_csv("additional.csv", sep = ";", index = False)
#z[ix].to_excel("additional.xlsx", index = False)


#s.to_csv("with_debt.csv",sep=";", index=False)
#s.to_excel("with_debt.xlsx")


#"Предэкспортное финансирование контракта на поставку спецтехники 
#(заемщик - ПАО «Нижегородский машиностроительный завод»)
#г. Нижний Новгород"
################################################################################################################################################################################################################################################################
#"Предоставление кредитных линий предприятиям группы «Русагро» в целях реструктуризации долга предприятий группы «РАЗГУЛЯЙ»
#(заемщик - ООО «Группа Компаний «Русагро»)
#п. Коммунарка"
#"Финансирование текущей деятельности ООО «НК «Северное сияние» по добыче и разведке углеводородного сырья на Мусюршорском, Лыдушорском и Шорсандивейском месторождениях
#(инициатор/заемщик - 
#ООО «НК «Северное сияние»)
#г. Нарьян-Мар"

#"Строительство олимпийского объекта «Ледовая арена для керлинга»
#(инициатор/заемщик - ООО «Инвестиционно-строительная компания «Славобласть»)
#г. Сочи"

#АГЕНТСТВО ПО ИПОТЕЧНОМУ ЖИЛИЩНОМУ  КРЕДИТОВАНИЮ, ОАО

#"Реконструкция предприятия по производству крупного вагонного литья на мощностях ООО «ВКМ-Сталь»
#(государственная гарантия Российской Федерации на сумму, составляющую 50% от суммы предоставленного кредита)
#(инициатор/заемщик - ООО «ВКМ-Сталь»)
#г. Саранск"

#"Строительство ПГУ-110 на Вологодской ТЭЦ установленной мощностью 110 МВт
#(инициатор - ОАО «ТГК-2», заемщики - ОАО «ВЭБ-лизинг», ОАО «ТГК-2»)
#г. Вологда"

#"Реконструкция Нижнетуринской ГРЭС
#(инициатор - ЗАО «Комплексные энергетические системы», заемщик - ПАО «Т Плюс» правопреемник ОАО «Волжская ТГК», являющегося правопреемником ОАО «ТГК-9»)
#г. Нижняя Тура, Свердловская область"

#"Строительство ПГУ-ТЭЦ установленной электрической мощностью 44 МВт и тепловой мощностью 26 Гкал/ч
#(инициатор/заемщик - АО «ГК-4»)
#г. Знаменск, Астраханская область"

#"Строительство и реконструкция малых гидроэлектростанций на территории Республики Карелия
#(инициатор/заемщик - АО «Норд Гидро»)"

#"Реконструкция Ижевской ТЭЦ-1 (инициатор - ЗАО «КЭС», заемщик - ПАО «Т Плюс» правопреемник ОАО «Волжская ТГК», являющегося правопреемником ОАО «ТГК-5»)
#г. Ижевск"

#"Создание инновационного строительного технопарка «Казбек» на территории Чеченской Республики
#(инициатор - ООО «Производственно-коммерческая фирма «Казбек», заемщик - ЗАО «Инновационный строительный технопарк «Казбек»)
#с. Беной, Ножай-Юртовский район"

#Строительство ГТЭС (ПГУ) "Молжаниновка"

#"Реконструкция и модернизация предприятий нефтеперерабатывающей промышленности Республики Сербской (Босния и Герцеговина) 
#(инициатор/заемщик - 
#АО «Нефтегазинкор» дочерняя компания АО «Зарубежнефть»)
#г. Баня-Лука"

#"Разработка и постановка на производство ЗРПК «Панцирь-СМ» 
#(инициатор/заемщик - 
#АО «Конструкторское бюро приборостроения им. академика А.Г. Шипунова»)
#г. Тула"

#"Финансирование расходов, связанных с выполнением (реализацией) государственного оборонного заказа на основе государственного контракта 
#№ З-1/6-30-14 от 26.03.2014 г.
#(заемщик - ПАО «Долгопрудненское научно-производственное предприятие»)
#г. Долгопрудный"
#"Предоставление кредита для целей исполнения контракта на выполнение работ по сервисному обслуживанию и ремонту вооружения, военной и специальной техники Вооруженных Сил РФ
#(заемщик - АО «Уральский завод транспортного машиностроения»)
#г. Екатеринбург"
#"Предоставление кредита ЗАО «Центр высокопрочных материалов «Армированные композиты» (ЗАО «ЦВМ «Армоком») в целях исполнения государственного контракта в рамках государственного оборонного заказа
#(заемщик - ЗАО «ЦВМ «Армоком»)
#Московская область"
#"Строительство и эксплуатация поселка «Горки-8»
#(заемщик - ООО «Горки-8»)"
#"Строительство отдельного промышленного производства метилхлорсиланов
#(инициатор - ОАО «Казанский завод синтетического каучука»,
#заемщик - АО «КЗСК-Силикон»)
#г. Казань"
#"Создание производства авиационных агрегатов в Першинском филиале 
#ОАО НПО «Наука»
#(инициатор/заемщик - ОАО НПО «Наука»)

#Владимирская область"
################################################################################################################################################################################################################################################################
################################################################################################################################################################################################################################################################
#Модернизация и развитие производственных мощностей в целях выпуска новых моделей автомобилей брендов LADA ,Renault, nissan 
#"Предоставление кредитной линии АО «Адмиралтейские верфи» в рамках Федеральной целевой программы «Развитие оборонно-промышленного комплекса РФ на 2011-2020 годы»
#(заемщик - АО «Адмиралтейские верфи»)

#г. Санкт-Петербург"
#"Предоставление кредита АО «НПК «Уралвагонзавод» в рамках Федеральной целевой программы «Развитие оборонно-промышленного комплекса РФ на 2011-2020 годы»
#(заемщик - АО «НПК «Уралвагонзавод»)
#г. Нижний Тагил"
################################################################################################################################################################################################################################################################

