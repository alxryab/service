const pgp = require('pg-promise')();
const connectionString = process.env.PG_CONN_STRING;
const db = pgp(connectionString);

console.log()
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function update(idStart,idEnd){
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
      operation.unit.id between ${idStart} and ${idEnd};
  `); 
}

async function updateLinks (){
  try{
    const maxId = await db.query('select max(id) from operation.unit');
    const updatesAmount = Math.ceil(maxId[0].max/100);

    let idStart = 1;
    let idEnd = 100;
    let updatedSuccess;

    for (i = 0; i < updatesAmount; i++ ) {
      await sleep(1000);
      try {
        await update(idStart,idEnd);
        updatedSuccess = true;
      } catch (error) {
        updatedSuccess = false;
        if (error.code ='40P01') {
          for (i = 0; i < 10; i++) {
            try {
              await update(idStart,idEnd);
              await sleep(1000);
              updatedSuccess = true;
              break;
            } catch (error) {
              console.log(error);
              console.log(error.where);
            }
          }
        } else {
          console.log(error)
          console.log(error.where);
        }
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