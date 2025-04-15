/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

select category.category_id,category.name,count(film_category.film_id) as filmcount from category
join film_category on category.category_id = film_category.category_id
group by category.category_id,category.name
order by filmcount desc;


--Варіант з віконною ф-цією
select distinct category.category_id,category.name,count(film_category.film_id) over (partition by category.category_id) as filmcount from category
join film_category on category.category_id = film_category.category_id
order by filmcount desc;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

---на випадок якщо мається на увазі поле rental_rate
/*select actor.actor_id,actor.first_name,actor.last_name,sum(film.rental_rate) as ratesum from film
join film_actor on film_actor.film_id = film.film_id
join actor on actor.actor_id = film_actor.actor_id
group by actor.actor_id,actor.first_name,actor.last_name
order by ratesum desc
limit 10*/


select actor.actor_id,actor.first_name,actor.last_name,count(inventory.film_id) as countofrentedfilms from actor
join film_actor on film_actor.actor_id = actor.actor_id
join inventory on film_actor.film_id = inventory.film_id
join rental on rental.inventory_id = inventory.inventory_id
group by actor.actor_id, actor.first_name, actor.last_name
order by countofrentedfilms desc
limit 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/




with filmcosts as
         (
select category.category_id,category.name,sum(film.replacement_cost) as costs from film
join film_category on film.film_id = film_category.film_id
join category on category.category_id = film_category.category_id
group by category.category_id, category.name
)

select category_id,name, costs from filmcosts
order by costs desc
limit 1;






/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/


select distinct film.title from film
left join inventory on inventory.film_id = film.film_id
where inventory.film_id is null;



/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
select actor.actor_id,actor.first_name,actor.last_name,count(category.category_id) as appearences from actor
join film_actor on film_actor.actor_id = actor.actor_id
join film on film.film_id = film_actor.film_id
join film_category on film.film_id = film_category.film_id
join category on film_category.category_id = category.category_id
where category.name = 'Children'
group by actor.actor_id, actor.first_name, actor.last_name
order by appearences desc
limit 3
;

