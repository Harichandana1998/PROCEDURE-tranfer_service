CREATE OR REPLACE PROCEDURE tranfer_service ( 
	v_source_aircraft_id int, 
	v_destination_aircraft_id int,
	v_service_id int )
language plpgsql
security definer
as$procedure$
declare
v_asset_id int;
v_status int;
v_customer_id int;
v_dest_customer_id int;
v_dest_asset_id int;

cur cursor FOR
select s.asset_id,s.status,a.custmer_id from service s join aircraft a on s.aircraft_id=a.aircraft_id where s.service_id=v_service_id
s.aircraft_id=v_source_aircraft_id;

BEGIN
open cur;

fetch cur into v_asset_id,v_status,v_customer_id;
close cur;
	if (v_asset_id is null) then 
		raise exception 'service is not found';
	end if;
	if (v_asset_id = 'In Progress') then 
		raise exception 'can not transeper the service';
	end if;
	
	select custmer_id into v_dest_asset_id from assets 
	where aircraft _id=v_destination_aircraft_id
	and asset_type = (select asset_type from assets where asset_id=v_asset_id);
	
	if (v_dest_asset_id is null) then 
	
		insert into assets (aircraft_id, asset_type, serial_number, installed_date)
		values (v_destination_aircraft_id, (select asset_type from assets where asset_id=v_asset_id),'new_assigned',now())
		returning asset_id into v_dest_asset_id;
	end if;
	
	update service 
	set aircraft_id = v_destination_aircraft_id
		asset_id=v_asset_id
		service_date=now()
		where service_id=v_service_id;
		
		insert into service_transfer_audit(service_id,from_aircraft,to_aircraft ,transfer_date)
		values(v_service_id,v_source_aircraft_id,v_destination_aircraft_id,now())
	
	exception 
	when others then 
	raise exception 'error :%,sqlstate=%',sqlerrm,sqlstate,;

end;

as$procedure$;


calling :

call tranfer_service(101,201,1234);



1 . select service_id,count(*) as transfer_count
from service_transfer_audit
where transfer_date>=now()-INTERVAL '1 year'
group by service_id 
order by transfer_count desc
limit 5;

2. select service_id,count(*) as transfer_count
from service_transfer_audit
where transfer_date>=now()-INTERVAL '30 days'
group by service_id 
having count(*)>1
limit 5;
