const pgp = require('pg-promise')();
const connectionString = process.env.PG_CONN_STRING;
const db = pgp(connectionString);

console.log()
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function update(idsToUpdate){
  return await db.query(`
    update 
      operation.operation_unit_link
    set 
      du_date = operation.unit.du_date,
      date_do1 = operation.unit.date_do1
    from operation.unit
    where 
      operation.unit.id  = operation.operation_unit_link.id_unit and
      operation.unit.deleted = 0 and
      operation.unit.id in (${idsToUpdate.join(',')});
  `);
}

async function updateLinks (){
  try{
    let units = await db.query('select id from operation.unit');
    const updatesAmount = Math.ceil(units.length/100);
    units = units.map(unit => unit.id)

    let updatedSuccess;
    let idsToUpdate = [], j, k = -1
    
    let idStart = 1;
    let idEnd = 100;

    for (j = 0; j < units.length; j++){
      if( j % 100 === 0){
        k++;
        idsToUpdate[k]=[]
      }
      idsToUpdate[k].push(units[j]); 
    }

    for (i = 0; i < updatesAmount; i++ ) {
      await sleep(10);
      try {
        await update(idsToUpdate[i]);
        updatedSuccess = true;
      } catch (error) {
        console.log(error);
        console.log(error.where);
        updatedSuccess = false;
      }

      if ( updatedSuccess ) {
        console.log(`units links updated ${idStart} and ${idEnd}`);
      } else {
        console.log(`units links NOT updated ${idStart} and ${idEnd}`);
      }

      idStart += 100;
      idEnd += 100;
    }
  } catch (error) {
    console.log(error);
    console.log(error.where);
  } finally {
    pgp.end(); // Закрываем соединение, когда закончим
  }
}

updateLinks();