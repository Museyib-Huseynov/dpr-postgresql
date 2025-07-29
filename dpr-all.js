import dotenv from 'dotenv';
dotenv.config();

import {
  promises as fs,
  createWriteStream,
  existsSync,
  mkdirSync,
  rmSync,
} from 'fs';
import path from 'path';
import officeCrypto from 'officecrypto-tool';
import readXlsxFile from 'read-excel-file/node';

import pg from 'pg';
const { Pool } = pg;

const inputFolder = process.argv[2] || './';
const password = process.env.EXCEL_PASSWORD;

//// generate logger function to log to console and local file
if (!existsSync('./logs')) {
  mkdirSync('./logs', { recursive: true });
}

if (existsSync('./QC')) {
  rmSync('./QC', { recursive: true, force: true });
}
mkdirSync('./QC', { recursive: true });

const logFilePath = getFormattedFilename('logs');
const QCFilePath = getFormattedFilename('QC');

const logStream = createWriteStream(logFilePath, { flags: 'a' });
const QCStream = createWriteStream(QCFilePath, { flags: 'a' });

const success = 'SUCCESS';
const error = 'ERROR';
const warning = 'WARNING';

let success_count = 0;
let error_count = 0;
let warning_count = 0;
let info_count = 0;

let flowmeter_insertion_count = 0;
let well_stock_insertion_count = 0;
let completion_insertion_count = 0;
let well_tests_insertion_count = 0;
let gas_well_tests_insertion_count = 0;
let daily_well_parameters_insertion_count = 0;
let well_downtime_reasons_insertion_count = 0;
let laboratory_results_insertion_count = 0;
let daily_general_comments_insertion_count = 0;

const logger = {
  log: (
    message,
    level = 'INFO',
    logger_without_timestamp_and_level = false,
    QC = false
  ) => {
    const timestamp = new Date().toLocaleString('en-US', {
      timeZone: 'Asia/Baku',
      hour12: false,
    });

    if (level === success) {
      success_count++;
    } else if (level === error) {
      error_count++;
    } else if (level === warning) {
      warning_count++;
    } else if (
      level === 'INFO' &&
      logger_without_timestamp_and_level === false
    ) {
      info_count++;
    }

    let content;
    if (logger_without_timestamp_and_level) {
      content = `\n${message}`;
    } else {
      content = `\n${timestamp} ${`[${level}]:`.padStart(10)} ${message}`;
    }

    logStream.write(content);
    console.log(content);

    if (QC == true) {
      QCStream.write(content);
    }
  },
  close: () => {
    logStream.end();
    QCStream.end();
  },
};
////

const missing_data = ['y', 'Y'];

let pool, client;
try {
  logger.log(`${'*'.repeat(100)}`, 'INFO', true);
  logger.log('SCRIPT EXECUTION STARTED...', 'INFO', true);

  pool = new Pool({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT),
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    ssl: false,
  });

  client = await pool.connect();

  await client.query('SET search_path To dpr');

  const { rows: fields } = await client.query('SELECT * FROM fields');

  const { rows: platforms } = await client.query('SELECT * FROM platforms');

  const { rows: wells } = await client.query('SELECT * FROM wells');

  const { rows: well_stock_categories } = await client.query(
    'SELECT * FROM well_stock_categories'
  );

  const { rows: production_well_stock_sub_categories } = await client.query(
    'SELECT * FROM production_well_stock_sub_categories'
  );

  const { rows: production_methods } = await client.query(
    'SELECT * FROM production_methods'
  );

  const { rows: horizons } = await client.query('SELECT * FROM horizons');

  const { rows: production_sub_skins_activities } = await client.query(
    'SELECT * FROM production_sub_skins_activities'
  );
  ////

  async function processFiles(folder) {
    //// Go through files in folder specified in `folder`, find `.xls` or `xlsx` files, decrypt them, parse each cell from Excel files, insert into db
    const files = await fs.readdir(folder, { withFileTypes: true });

    outer: for (const file of files) {
      const filePath = path.join(folder, file.name);
      if (file.isDirectory()) {
        await processFiles(filePath);
      } else {
        const extension = path.extname(file.name).toLowerCase();
        if (extension === '.xls' || extension === '.xlsx') {
          logger.log(`${'='.repeat(100)}`, 'INFO', true);
          logger.log(`|'${filePath}'| Parsing...`, 'INFO', true, true);

          const input = await fs.readFile(filePath);
          const isEncrypted = officeCrypto.isEncrypted(input);
          let output;
          if (isEncrypted) {
            output = await officeCrypto.decrypt(input, { password });
            await fs.writeFile(filePath, output);
          } else {
            output = input;
          }
          const rows = await readXlsxFile(output, { sheet: 'Hesabat forması' });

          // parse field_id
          let field, field_id;
          if (rows[3][5] != 'LTS') {
            field = rows[3][5];

            field_id = fields.find((i) => i.name == field)?.id;
            if (!field_id) {
              logger.log(
                'Field name is not correct in excel file',
                error,
                false,
                true
              );
              logger.log(`Data is not persisted into DB!`, warning);
              continue outer;
            }
          }
          //

          // parse platform_id
          let platform, platform_id;
          if (rows[2][5] == '28 May') {
            platform = rows[4][5];
            platform_id = platforms.find((i) => {
              return i.name == platform && i.field_id == field_id;
            })?.id;
            if (!platform_id) {
              logger.log(
                `Platform number is not correct in excel file`,
                error,
                false,
                true
              );
              logger.log(`Data is not persisted into DB!`, warning);
              continue outer;
            }
          } else if (rows[3][5] != 'LTS') {
            platform = rows[4][5];
          }
          //

          // parse report_date
          let report_date = rows[6][5];
          if (!isValidDate(report_date)) {
            logger.log(`Report_date is not correct`, error, false, true);
            logger.log(`Data is not persisted into DB!`, warning);
            continue outer;
          }
          report_date = processDateValue(report_date);

          const get_report_date_id_query =
            'SELECT id FROM report_dates WHERE report_date = $1';

          const { rows: report_date_query_result } = await client.query(
            get_report_date_id_query,
            [report_date]
          );

          const { id: report_date_id } = report_date_query_result[0] || {};
          //

          // check if report is yesterday's report
          // const today = new Date().toLocaleDateString('en-CA', {
          //   timeZone: 'Asia/Baku',
          // });
          // const diffDays = (new Date(today) - new Date(report_date)) / 86400000;
          // if (diffDays !== 1) {
          //   logger.log(`Report_date is not yesterday's`, error, false, true);
          //   logger.log(`Data is not persisted into DB!`, warning);
          //   continue outer;
          // }
          //

          QC: for (let i = 14; i < rows.length; i++) {
            const row = rows[i];
            if (row[2] === null) {
              let errors = [
                rows[i + 7][6],
                rows[i + 8][6],
                rows[i + 9][6],
                rows[i + 10][6],
                rows[i + 11][6],
                rows[i + 12][6],
                rows[i + 13][6],
                rows[i + 14][6],
                rows[i + 15][6],
                rows[i + 16][6],
                rows[i + 17][6],
                rows[i + 18][6],
                rows[i + 19][6],
                rows[i + 20][6],
                rows[i + 21][6],
                rows[i + 22][6],
              ];

              errors = errors.map((i) => {
                return Boolean(i);
              });

              if (errors.includes(true)) {
                logger.log(
                  `There is an error in ${field} field - platform ${platform}`,
                  error,
                  false,
                  true
                );
                continue outer;
              }
              break QC;
            }
          }

          // rename excel file to keep it clean
          let pl = platform || 'LTS';
          const newFileName = `DPR-${pl}-${report_date}.xlsx`;
          const newFilePath = path.join(path.dirname(filePath), newFileName);
          await fs.rename(filePath, newFilePath);
          //

          // parse responsible_person
          let responsible_person = rows[3][18];
          //

          // parse phone_number
          let phone_number = rows[4][18];
          //

          // parse flowmeter params
          const reading1 = rows[3][61];
          const reading2 = rows[4][61];
          const reading3 = rows[3][63];
          const reading4 = rows[4][63];
          let calibration_date = rows[6][61];
          if (isValidDate(calibration_date)) {
            calibration_date = processDateValue(calibration_date);
          } else {
            calibration_date = null;
          }
          //

          //// populate flowmeters table
          logger.log(`${'-'.repeat(100)}`, 'INFO', true);
          logger.log(
            `|'Report Date: ${report_date}'|'Platform ${platform}'|'flowmeters table'| populating DB...`,
            'INFO',
            true
          );

          // check flowmeters entry exists in DB
          const flowmeters_entry_exists_query =
            'SELECT COUNT(*) AS flowmeters_entry_exists FROM flowmeters WHERE platform_id = $1 AND report_date_id = $2';

          const { rows: flowmeters_entry_exists_query_result } =
            await client.query(flowmeters_entry_exists_query, [
              platform_id,
              report_date_id,
            ]);

          const { flowmeters_entry_exists } =
            flowmeters_entry_exists_query_result[0] || {};
          //

          // get previous entry from flowmeters table
          const flowmeters_previous_entry_query = `
                  SELECT * 
                  FROM flowmeters 
                  WHERE platform_id = $1 
                    AND report_date_id < $2 
                  ORDER BY report_date_id DESC 
                  LIMIT 1;
                `;

          const { rows: flowmeters_previous_entry_query_result } =
            await client.query(flowmeters_previous_entry_query, [
              platform_id,
              report_date_id,
            ]);

          const {
            report_date_id: flowmeters_previous_entry_report_date_id,
            reading1: flowmeters_previous_entry_reading1,
            reading2: flowmeters_previous_entry_reading2,
            reading3: flowmeters_previous_entry_reading3,
            reading4: flowmeters_previous_entry_reading4,
          } = flowmeters_previous_entry_query_result[0] || {};

          const flowmeters_previous_entry_is_yesterday = isYesterday(
            report_date_id,
            flowmeters_previous_entry_report_date_id
          );
          //

          // insert entry into flowmeter table
          const flowmeters_insert_query =
            'INSERT INTO flowmeters (platform_id, report_date_id, reading1, reading2, reading3, reading4, calibration_date) VALUES ($1, $2, $3, $4, $5, $6, $7)';

          if (field_id == 1) {
            if (![2, 3, 4, 7, 8, 13].includes(platform)) {
              logger.log(`Flowmeter is not present`);
              logger.log(`Not populated!`);
            } else if (
              reading2 == null ||
              reading4 == null ||
              ([8, 13].includes(platform) &&
                (reading1 == null || reading3 == null))
            ) {
              logger.log(`Check flowmeter parameters`, error, false, true);
              logger.log(`Not populated!`, warning);
            }
            // else if (
            //   flowmeters_previous_entry_is_yesterday &&
            //   [8, 13].includes(platform) &&
            //   (flowmeters_previous_entry_reading2 != reading1 ||
            //     flowmeters_previous_entry_reading4 != reading3)
            // ) {
            //   logger.log(
            //     `Yesterday's flowmeter reading should be same with yesterday's, but different!`,
            //     error,
            //     false,
            //     true
            //   );
            // }
            else if (!Number(flowmeters_entry_exists)) {
              await client.query(flowmeters_insert_query, [
                platform_id,
                report_date_id,
                reading1,
                reading2,
                reading3,
                reading4,
                calibration_date,
              ]);
              logger.log(`Populated!`, success);
              flowmeter_insertion_count++;
              // check whether today's flowmeter params same as yesterday's (show warning)
              if (
                flowmeters_previous_entry_is_yesterday &&
                [2, 3, 4, 7].includes(platform) &&
                (flowmeters_previous_entry_reading2 == reading2 ||
                  flowmeters_previous_entry_reading4 == reading4)
              ) {
                logger.log(
                  `Today's Flowmeter params are same as yesterday's!`,
                  warning,
                  false,
                  true
                );
              }
              //
            } else {
              logger.log(`Already populated!`);
            }
          } else {
            logger.log(`Flowmeter is not present`);
            logger.log(`Not populated!`);
          }

          //
          ////

          //// looping through rows (wells)
          inner: for (let i = 14; i < rows.length; i++) {
            logger.log(`${'-'.repeat(100)}`, 'INFO', true);
            const row = rows[i];

            if (row[2] === null) {
              let general_comments = rows[i + 7][26];

              //// populate daily_general_comments table
              // check daily_general_comments entry exists in DB
              const daily_general_comments_entry_exists_query =
                'SELECT COUNT(*) AS daily_general_comments_entry_exists FROM daily_general_comments WHERE field_id=$1 AND report_date_id=$2 AND platform=$3';

              const { rows: daily_general_comments_entry_exists_query_result } =
                await client.query(daily_general_comments_entry_exists_query, [
                  field_id,
                  report_date_id,
                  platform,
                ]);

              const { daily_general_comments_entry_exists } =
                daily_general_comments_entry_exists_query_result[0] || {};
              //

              // get previous entry from daily_general_comments
              const daily_general_comments_previous_entry_query =
                'SELECT * FROM daily_general_comments WHERE field_id = $1 AND platform = $2 AND report_date_id < $3 ORDER BY report_date_id DESC LIMIT 1';

              const {
                rows: daily_general_comments_previous_entry_query_result,
              } = await client.query(
                daily_general_comments_previous_entry_query,
                [field_id, platform, report_date_id]
              );

              const {
                general_comments:
                  daily_general_comments_previous_entry_general_comments,
              } = daily_general_comments_previous_entry_query_result[0] || {};

              const general_comment_changed =
                daily_general_comments_previous_entry_general_comments !=
                general_comments;
              //

              // insert entry into daily_general_comments table
              const daily_general_comments_insert_query =
                'INSERT INTO daily_general_comments (report_date_id, field_id, platform, general_comments) VALUES ($1, $2, $3, $4)';

              if (
                !Number(daily_general_comments_entry_exists) &&
                general_comment_changed
              ) {
                await client.query(daily_general_comments_insert_query, [
                  report_date_id,
                  field_id,
                  platform,
                  general_comments,
                ]);
                logger.log(`|'daily_general_comments'| Populated!`, success);
                daily_general_comments_insertion_count++;
              } else {
                logger.log(`|'daily_general_comments'| Already populated!`);
              }
              //
              ////
              break;
            }

            let validation_error = false;

            if (rows[3][5] == 'LTS') {
              if (row[2] == '3') {
                field = 'Palçıq Pilpiləsi';
              } else {
                field = 'Neft Daşları';
              }

              field_id = fields.find((i) => i.name == field)?.id;
            }

            let square = row[3];

            if (rows[2][5] != '28 May') {
              if (rows[3][5] == 'LTS') {
                platform = row[2];
              }

              platform_id = platforms.find((i) => {
                return (
                  i.name == platform &&
                  i.field_id == field_id &&
                  i.square == square
                );
              })?.id;
            }

            const well_number = row[4];
            const well_id = wells.find((i) => {
              return (
                i.name.trim() == String(well_number).trim() &&
                i.platform_id == platform_id
              );
            })?.id;

            // check if well name is specified correctly
            if (!well_id) {
              logger.log(
                `Check |'Platform ${platform}'|'row-${
                  i + 1
                }'| Well name is not correct`,
                error,
                false,
                true
              );
              logger.log(
                `Check |'Platform ${platform}'|'row-${
                  i + 1
                }'| Data is not persisted into DB!`,
                warning
              );
              continue inner;
            }
            //

            logger.log(
              `|'Report Date: ${report_date}'|'Platform ${platform}'|'Well ${well_number}'| populating DB...`,
              'INFO',
              true
            );

            const well_stock_category_id = well_stock_categories.find(
              (i) => i.name.trim() === row[5]?.trim?.()
            )?.id;
            const production_well_stock_sub_category_id =
              production_well_stock_sub_categories.find(
                (i) => i.name.trim() === row[6]?.trim?.()
              )?.id;
            const production_method_id = production_methods.find(
              (i) => i.name.trim() === row[7]?.trim?.()
            )?.id;
            const horizon_id = horizons.find(
              (i) => i.name.trim() === row[8]?.trim?.()
            )?.id;

            const casing = row[9];
            const completion_interval = row[10];
            const tubing1_depth = row[11];
            const tubing1_length = row[12];
            const tubing2_depth = row[13];
            const tubing2_length = row[14];
            const tubing3_depth = row[15];
            const tubing3_length = row[16];
            const packer_depth = row[17];
            const flowmeter = row[18];

            let last_well_test_date = row[19];
            let last_gas_well_test_date = row[23];
            let last_lab_date = row[26];

            let last_well_test_date_id,
              last_gas_well_test_date_id,
              last_lab_date_id;

            // check if dates are in correct format
            if (
              !isValidDate(last_well_test_date) ||
              !isValidDate(last_gas_well_test_date) ||
              !isValidDate(last_lab_date)
            ) {
              logger.log(
                `Check 'last_well_test_date or last_lab_date is not correct'`,
                error,
                false,
                true
              );
              validation_error = true;
            } else {
              last_well_test_date = processDateValue(last_well_test_date);
              last_gas_well_test_date = processDateValue(
                last_gas_well_test_date
              );
              last_lab_date = processDateValue(last_lab_date);

              last_well_test_date_id = Number(
                (
                  await client.query(
                    `SELECT id FROM report_dates WHERE report_date=${last_well_test_date}`
                  )
                ).rows[0].id
              );
              last_gas_well_test_date_id = Number(
                (
                  await client.query(
                    `SELECT id FROM report_dates WHERE report_date=${last_gas_well_test_date}`
                  )
                ).rows[0].id
              );
              last_lab_date_id = Number(
                (
                  await client.query(
                    `SELECT id FROM report_dates WHERE report_date=${last_lab_date}`
                  )
                ).rows[0].id
              );
            }
            //

            const liquid_ton =
              row[20] == null ||
              row[20] === '' ||
              isNaN(Number(String(row[20]).replace(/,/g, '.')))
                ? null
                : Number(String(row[20]).replace(/,/g, '.'));
            const oil_ton =
              row[21] == null ||
              row[21] === '' ||
              isNaN(Number(String(row[21]).replace(/,/g, '.')))
                ? null
                : Number(String(row[21]).replace(/,/g, '.'));
            const water_ton =
              row[22] == null ||
              row[22] === '' ||
              isNaN(Number(String(row[22]).replace(/,/g, '.')))
                ? null
                : Number(String(row[22]).replace(/,/g, '.'));
            const total_gas =
              row[24] == null ||
              row[24] === '' ||
              isNaN(Number(String(row[24]).replace(/,/g, '.')))
                ? null
                : Number(String(row[24]).replace(/,/g, '.'));
            const gaslift_gas_wt =
              row[25] == null ||
              row[25] === '' ||
              isNaN(Number(String(row[25]).replace(/,/g, '.')))
                ? null
                : Number(String(row[25]).replace(/,/g, '.'));

            const water_cut =
              row[27] == null ||
              row[27] === '' ||
              isNaN(Number(String(row[27]).replace(/,/g, '.')))
                ? null
                : Number(String(row[27]).replace(/,/g, '.'));
            const mechanical_impurities =
              row[28] == null ||
              row[28] === '' ||
              isNaN(Number(String(row[28]).replace(/,/g, '.')))
                ? null
                : Number(String(row[28]).replace(/,/g, '.'));
            const pqa =
              missing_data.includes(row[29]) ||
              row[29] == null ||
              row[29] === '' ||
              isNaN(Number(String(row[29]).replace(/,/g, '.')))
                ? null
                : String(row[29]).replace(/,/g, '.');
            const phf =
              missing_data.includes(row[30]) ||
              row[30] == null ||
              row[30] === '' ||
              isNaN(Number(String(row[30]).replace(/,/g, '.')))
                ? null
                : String(row[30]).replace(/,/g, '.');
            const pba =
              missing_data.includes(row[31]) ||
              row[31] == null ||
              row[31] === '' ||
              isNaN(Number(String(row[31]).replace(/,/g, '.')))
                ? null
                : String(row[31]).replace(/,/g, '.');
            const p6x9 =
              missing_data.includes(row[32]) ||
              row[32] == null ||
              row[32] === '' ||
              isNaN(Number(String(row[32]).replace(/,/g, '.')))
                ? null
                : String(row[32]).replace(/,/g, '.');
            const p9x13 =
              missing_data.includes(row[33]) ||
              row[33] == null ||
              row[33] === '' ||
              isNaN(Number(String(row[33]).replace(/,/g, '.')))
                ? null
                : String(row[33]).replace(/,/g, '.');
            const p13x20 =
              missing_data.includes(row[34]) ||
              row[34] == null ||
              row[34] === '' ||
              isNaN(Number(String(row[34]).replace(/,/g, '.')))
                ? null
                : String(row[34]).replace(/,/g, '.');
            const choke =
              missing_data.includes(row[35]) ||
              row[35] == null ||
              row[35] === '' ||
              isNaN(Number(String(row[35]).replace(/,/g, '.')))
                ? null
                : String(row[35]).replace(/,/g, '.');
            const gaslift_gas_day =
              missing_data.includes(row[36]) ||
              row[36] == null ||
              row[36] === '' ||
              isNaN(Number(String(row[36]).replace(/,/g, '.')))
                ? null
                : Number(String(row[36]).replace(/,/g, '.'));
            const gaslift_system_pressure =
              missing_data.includes(row[37]) ||
              row[37] == null ||
              row[37] === '' ||
              isNaN(Number(String(row[37]).replace(/,/g, '.')))
                ? null
                : row[37];
            const pump_depth =
              missing_data.includes(row[38]) ||
              row[38] == null ||
              row[38] === '' ||
              isNaN(Number(String(row[38]).replace(/,/g, '.')))
                ? null
                : Number(String(row[38]).replace(/,/g, '.'));
            const pump_frequency =
              missing_data.includes(row[39]) ||
              row[39] == null ||
              row[39] === '' ||
              isNaN(Number(String(row[39]).replace(/,/g, '.')))
                ? null
                : Number(String(row[39]).replace(/,/g, '.'));
            const pump_hydrostatic_pressure =
              missing_data.includes(row[40]) ||
              row[40] == null ||
              row[40] === '' ||
              isNaN(Number(String(row[40]).replace(/,/g, '.')))
                ? null
                : Number(String(row[40]).replace(/,/g, '.'));
            const esp_pump_size =
              missing_data.includes(row[41]) ||
              row[41] == null ||
              row[41] === '' ||
              isNaN(Number(String(row[41]).replace(/,/g, '.')))
                ? null
                : Number(String(row[41]).replace(/,/g, '.'));
            const esp_pump_stages =
              missing_data.includes(row[42]) ||
              row[42] == null ||
              row[42] === '' ||
              isNaN(Number(String(row[42]).replace(/,/g, '.')))
                ? null
                : Number(String(row[42]).replace(/,/g, '.'));
            const esp_pump_rate =
              missing_data.includes(row[43]) ||
              row[43] == null ||
              row[43] === '' ||
              isNaN(Number(String(row[43]).replace(/,/g, '.')))
                ? null
                : Number(String(row[43]).replace(/,/g, '.'));
            const esp_pump_head =
              missing_data.includes(row[44]) ||
              row[44] == null ||
              row[44] === '' ||
              isNaN(Number(String(row[44]).replace(/,/g, '.')))
                ? null
                : Number(String(row[44]).replace(/,/g, '.'));
            const esp_downhole_gas_separator =
              missing_data.includes(row[45]) ||
              row[45] == null ||
              row[45] === '' ||
              isNaN(Number(String(row[45]).replace(/,/g, '.')))
                ? null
                : row[45];
            const srp_pumpjack_type =
              missing_data.includes(row[46]) ||
              row[46] == null ||
              row[46] === '' ||
              isNaN(Number(String(row[46]).replace(/,/g, '.')))
                ? null
                : row[46];
            const srp_pump_plunger_diameter =
              missing_data.includes(row[47]) ||
              row[47] == null ||
              row[47] === '' ||
              isNaN(Number(String(row[47]).replace(/,/g, '.')))
                ? null
                : Number(String(row[47]).replace(/,/g, '.'));
            const srp_plunger_stroke_length =
              missing_data.includes(row[48]) ||
              row[48] == null ||
              row[48] === '' ||
              isNaN(Number(String(row[48]).replace(/,/g, '.')))
                ? null
                : Number(String(row[48]).replace(/,/g, '.'));
            const srp_balancer_oscillation_frequency =
              missing_data.includes(row[49]) ||
              row[49] == null ||
              row[49] === '' ||
              isNaN(Number(String(row[49]).replace(/,/g, '.')))
                ? null
                : Number(String(row[49]).replace(/,/g, '.'));
            const srp_pump_rate_coefficient =
              missing_data.includes(row[50]) ||
              row[50] == null ||
              row[50] === '' ||
              isNaN(Number(String(row[50]).replace(/,/g, '.')))
                ? null
                : Number(String(row[50]).replace(/,/g, '.'));
            const srp_max_motor_speed =
              missing_data.includes(row[51]) ||
              row[51] == null ||
              row[51] === '' ||
              isNaN(Number(String(row[51]).replace(/,/g, '.')))
                ? null
                : Number(String(row[51]).replace(/,/g, '.'));
            const srp_shaft_diameter =
              missing_data.includes(row[52]) ||
              row[52] == null ||
              row[52] === '' ||
              isNaN(Number(String(row[52]).replace(/,/g, '.')))
                ? null
                : Number(String(row[52]).replace(/,/g, '.'));
            const pcp_pump_rate =
              missing_data.includes(row[53]) ||
              row[53] == null ||
              row[53] === '' ||
              isNaN(Number(String(row[53]).replace(/,/g, '.')))
                ? null
                : Number(String(row[53]).replace(/,/g, '.'));
            const pcp_rpm =
              missing_data.includes(row[54]) ||
              row[54] == null ||
              row[54] === '' ||
              isNaN(Number(String(row[54]).replace(/,/g, '.')))
                ? null
                : Number(String(row[54]).replace(/,/g, '.'));
            const pcp_screw_diameter =
              missing_data.includes(row[55]) ||
              row[55] == null ||
              row[55] === '' ||
              isNaN(Number(String(row[55]).replace(/,/g, '.')))
                ? null
                : Number(String(row[55]).replace(/,/g, '.'));
            const static_fluid_level =
              missing_data.includes(row[56]) ||
              row[56] == null ||
              row[56] === '' ||
              isNaN(Number(String(row[56]).replace(/,/g, '.')))
                ? null
                : Number(String(row[56]).replace(/,/g, '.'));
            const dynamic_fluid_level =
              missing_data.includes(row[57]) ||
              row[57] == null ||
              row[57] === '' ||
              isNaN(Number(String(row[57]).replace(/,/g, '.')))
                ? null
                : Number(String(row[57]).replace(/,/g, '.'));

            const well_uptime_hours =
              missing_data.includes(row[58]) ||
              row[58] == null ||
              row[58] === '' ||
              isNaN(Number(String(row[58]).replace(/,/g, '.')))
                ? null
                : Number(String(row[58]).replace(/,/g, '.'));
            const downtime_category = row[59];
            const production_sub_skins_activity_id =
              production_sub_skins_activities.find(
                (i) => i.name.slice(0, 5).trim() === row[62]?.slice(0, 5).trim()
              )?.id;
            const comments = row[63];

            // check if well is flowing, then flowmeter column is specified
            if (
              field_id === 1 &&
              (well_stock_category_id === 1 || well_stock_category_id === 2) &&
              production_well_stock_sub_category_id === 1 &&
              !flowmeter
            ) {
              logger.log(
                `Check 'which flowmeter well is flowing'`,
                error,
                false,
                true
              );
              validation_error = true;
            }
            //

            // check if well_uptime_hours is in correct format
            if (well_uptime_hours < 0 || well_uptime_hours > 24) {
              logger.log(
                `Check 'well_uptime_hours should be between (0-24) hours'`,
                error,
                false,
                true
              );
              validation_error = true;
            }
            //

            // check if well_uptime_hours less than 24, reasons are specified
            if (
              well_uptime_hours < 24 &&
              (!downtime_category ||
                !production_sub_skins_activity_id ||
                !comments)
            ) {
              logger.log(
                `Check 'well_uptime_hours < 24, bu no skin, no comment'`,
                error,
                false,
                true
              );
              validation_error = true;
            }
            //

            // check liquid_ton is not out of range
            if (
              (well_stock_category_id === 1 || well_stock_category_id === 2) &&
              liquid_ton > 500
            ) {
              logger.log(
                `Check 'liquid_ton can not be bigger than 500'`,
                error,
                false,
                true
              );
              validation_error = true;
            }
            //

            // check total_gas is bigger than gaslift_gas
            if (
              total_gas < gaslift_gas_wt
              // || (total_gas / 24) * well_uptime_hours < gaslift_gas_day
            ) {
              logger.log(
                `Check 'total gas can not be less than gaslift gas'`,
                error,
                false,
                true
              );
              validation_error = true;
            }
            //

            // check water_cut and mechanical_impurities is not out of range
            if (
              water_cut < 0 ||
              water_cut > 100 ||
              mechanical_impurities < 0 ||
              mechanical_impurities > 100
            ) {
              logger.log(
                `Check 'water cut / mechanical impurities should be between (0-100)%'`,
                error,
                false,
                true
              );
              validation_error = true;
            }
            //

            // check whether last_lab_date belongs to well_test
            // const well_tests_first_entry_query =
            //   'SELECT * FROM well_tests WHERE well_id = $1 ORDER BY well_test_date TOP 1';

            // const {rows: well_tests_first_entry_query_result} = await client.query(well_tests_first_entry_query, [well_id])

            // const { well_test_date: well_tests_first_entry_report_date } =
            //   well_tests_first_entry_query_result[0] || {};

            // if (well_tests_first_entry_report_date) {
            //   const lab_result_exists_query =
            //     'SELECT COUNT(*) AS well_tests_count FROM well_tests WHERE well_id = $1 AND (well_test_date = $2 OR (($4::date - $3::date) BETWEEN 0 AND 1) OR $5 < $6)';

            // const {rows: lab_result_exists_query_result} = await client.query(lab_result_exists_query, [well_id, last_lab_date_id, last_well_test_date, last_lab_date, last_lab_date, well_tests_first_entry_report_date])

            //   const { well_tests_count } = lab_result_exists_query_result[0] || {};

            //   if (!Number(well_tests_count)) {
            //     logger.log(
            //       `last_lab_date does not belong to past well_tests`,
            //       error
            //     );
            //     validation_error = true;
            //   }
            // }
            //

            // important, all errors rejects here
            if (validation_error) {
              logger.log(`Data is not persisted into DB!`, warning);
              continue inner;
            }
            //

            //// populate well_stock table
            // check well_stock entry exists in DB
            const well_stock_entry_exists_query =
              'SELECT COUNT(*) AS well_stock_entry_exists FROM well_stock WHERE well_id = $1 AND report_date_id = $2';

            const { rows: well_stock_entry_exists_query_result } =
              await client.query(well_stock_entry_exists_query, [
                well_id,
                report_date_id,
              ]);

            const { well_stock_entry_exists } =
              well_stock_entry_exists_query_result[0] || {};
            //

            // get previous entry from well_stock table
            const well_stock_previous_entry_query =
              'SELECT * FROM well_stock WHERE well_id = $1 AND report_date_id < $2 ORDER BY report_date_id DESC LIMIT 1';

            const { rows: well_stock_previous_entry_query_result } =
              await client.query(well_stock_previous_entry_query, [
                well_id,
                report_date_id,
              ]);

            const {
              well_stock_category_id:
                well_stock_previous_entry_well_stock_category_id,
              production_well_stock_sub_category_id:
                well_stock_previous_entry_production_well_stock_sub_category_id,
              production_method_id:
                well_stock_previous_entry_production_method_id,
            } = well_stock_previous_entry_query_result[0] || {};
            //

            // insert entry into well_stock table
            const well_stock_sub_category_id = 1;

            const well_stock_insert_query =
              'INSERT INTO well_stock (well_id, report_date_id, well_stock_category_id, well_stock_sub_category_id, production_well_stock_sub_category_id, production_method_id) VALUES ($1, $2, $3, $4, $5, $6)';

            const well_stock_changed =
              well_stock_previous_entry_well_stock_category_id !=
                well_stock_category_id ||
              well_stock_previous_entry_production_well_stock_sub_category_id !=
                production_well_stock_sub_category_id ||
              well_stock_previous_entry_production_method_id !=
                production_method_id;

            if (!Number(well_stock_entry_exists) && well_stock_changed) {
              await client.query(well_stock_insert_query, [
                well_id,
                report_date_id,
                well_stock_category_id,
                well_stock_sub_category_id,
                production_well_stock_sub_category_id,
                production_method_id,
              ]);
              logger.log(
                `|'well_stock'| Populated! Change in well ${well_number}`,
                warning,
                false,
                true
              );
              well_stock_insertion_count++;
            } else {
              logger.log(
                `|'well_stock'| Already populated! (or nothing changed compared to yesterday)`
              );
            }
            //
            ////

            //// populate completions table
            // check completions entry exists in DB
            const completions_entry_exists_query =
              'SELECT COUNT(*) AS completions_entry_exists FROM completions WHERE well_id = $1 AND report_date_id = $2';

            const { rows: completions_entry_exists_query_result } =
              await client.query(completions_entry_exists_query, [
                well_id,
                report_date_id,
              ]);

            const { completions_entry_exists } =
              completions_entry_exists_query_result[0] || {};
            //

            // get previous entry from completion table
            const completions_previous_entry_query =
              'SELECT * FROM completions WHERE well_id = $1 AND report_date_id < $2 ORDER BY report_date_id DESC LIMIT 1';

            const { rows: completions_previous_entry_query_result } =
              await client.query(completions_previous_entry_query, [
                well_id,
                report_date_id,
              ]);

            const {
              horizon_id: completions_previous_entry_horizon_id,
              casing: completions_previous_entry_casing,
              completion_interval:
                completions_previous_entry_completion_interval,
              tubing1_depth: completions_previous_entry_tubing1_depth,
              tubing1_length: completions_previous_entry_tubing1_length,
              tubing2_depth: completions_previous_entry_tubing2_depth,
              tubing2_length: completions_previous_entry_tubing2_length,
              tubing3_depth: completions_previous_entry_tubing3_depth,
              tubing3_length: completions_previous_entry_tubing3_length,
              packer_depth: completions_previous_entry_packer_depth,
            } = completions_previous_entry_query_result[0] || {};
            //

            // insert entry into completions table
            const completions_insert_query =
              'INSERT INTO completions (well_id, report_date_id, horizon_id, casing, completion_interval, tubing1_depth, tubing1_length, tubing2_depth, tubing2_length, tubing3_depth, tubing3_length, packer_depth) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)';

            const completion_changed =
              completions_previous_entry_horizon_id != horizon_id ||
              completions_previous_entry_casing != casing ||
              completions_previous_entry_completion_interval !=
                completion_interval ||
              completions_previous_entry_tubing1_depth != tubing1_depth ||
              completions_previous_entry_tubing1_length != tubing1_length ||
              completions_previous_entry_tubing2_depth != tubing2_depth ||
              completions_previous_entry_tubing2_length != tubing2_length ||
              completions_previous_entry_tubing3_depth != tubing3_depth ||
              completions_previous_entry_tubing3_length != tubing3_length ||
              completions_previous_entry_packer_depth != packer_depth;

            if (!Number(completions_entry_exists) && completion_changed) {
              await client.query(completions_insert_query, [
                well_id,
                report_date_id,
                horizon_id,
                casing,
                completion_interval,
                tubing1_depth,
                tubing1_length,
                tubing2_depth,
                tubing2_length,
                tubing3_depth,
                tubing3_length,
                packer_depth,
              ]);
              logger.log(
                `|'completions'| Populated! Change in well ${well_number}`,
                warning,
                false,
                true
              );
              completion_insertion_count++;
            } else {
              logger.log(
                `|'completions'| Already populated! (or nothing changed compared to yesterday)`
              );
            }
            //
            ////

            //// populate well_downtime_reasons table
            // check well_downtime_reasons entry exists in DB
            const well_downtime_reasons_entry_exists_query =
              'SELECT COUNT(*) AS well_downtime_reasons_entry_exists FROM well_downtime_reasons WHERE well_id = $1 AND report_date_id = $2';

            const { rows: well_downtime_reasons_entry_exists_query_result } =
              await client.query(well_downtime_reasons_entry_exists_query, [
                well_id,
                report_date_id,
              ]);

            const { well_downtime_reasons_entry_exists } =
              well_downtime_reasons_entry_exists_query_result[0] || {};
            //

            // get previous entry from well_downtime_reasons table
            const well_downtime_reasons_previous_entry_query =
              'SELECT * FROM well_downtime_reasons WHERE well_id = $1 AND report_date_id < $2 ORDER BY report_date_id DESC LIMIT 1';

            const { rows: well_downtime_reasons_previous_entry_query_result } =
              await client.query(well_downtime_reasons_previous_entry_query, [
                well_id,
                report_date_id,
              ]);

            const {
              downtime_category:
                well_downtime_reasons_previous_downtime_category,
              production_sub_skins_activity_id:
                well_downtime_reasons_previous_production_sub_skins_activity_id,
              comments: well_downtime_reasons_previous_comments,
            } = well_downtime_reasons_previous_entry_query_result[0] || {};
            //

            // insert entry into well_downtime_reasons table
            const well_downtime_reasons_insert_query =
              'INSERT INTO well_downtime_reasons (well_id, report_date_id, downtime_category, production_sub_skins_activity_id, comments) VALUES ($1, $2, $3, $4, $5)';

            const well_downtime_reasons_changed =
              well_downtime_reasons_previous_downtime_category !=
                downtime_category ||
              well_downtime_reasons_previous_production_sub_skins_activity_id !=
                production_sub_skins_activity_id ||
              well_downtime_reasons_previous_comments != comments;

            if (
              !Number(well_downtime_reasons_entry_exists) &&
              well_downtime_reasons_changed
            ) {
              await client.query(well_downtime_reasons_insert_query, [
                well_id,
                report_date_id,
                downtime_category,
                production_sub_skins_activity_id,
                comments,
              ]);
              logger.log(
                `|'well_downtime_reasons'| Populated! Change in well ${well_number}`,
                warning,
                false,
                true
              );
              well_downtime_reasons_insertion_count++;
            } else {
              logger.log(
                `|'well_downtime_reasons'| Already populated! (or well_uptime_hours = 24)`
              );
            }
            //
            ////

            //// populate daily_well_parameters table
            // check daily_well_parameters entry exists in DB
            const daily_well_parameters_entry_exists_query =
              'SELECT COUNT(*) AS daily_well_parameters_entry_exists FROM daily_well_parameters WHERE well_id = $1 AND report_date_id = $2';

            const { rows: daily_well_parameters_entry_exists_query_result } =
              await client.query(daily_well_parameters_entry_exists_query, [
                well_id,
                report_date_id,
              ]);

            const { daily_well_parameters_entry_exists } =
              daily_well_parameters_entry_exists_query_result[0] || {};
            //

            // insert entry into daily_well_parameters table
            const daily_well_parameters_insert_query =
              'INSERT INTO daily_well_parameters (well_id, report_date_id, flowmeter, well_uptime_hours, choke, pqa, phf, pba, p6x9, p9x13, p13x20, gaslift_gas, gaslift_system_pressure, pump_depth, pump_frequency, pump_hydrostatic_pressure, esp_pump_size, esp_pump_stages, esp_pump_rate, esp_pump_head, esp_downhole_gas_separator, srp_pumpjack_type, srp_pump_plunger_diameter, srp_plunger_stroke_length, srp_balancer_oscillation_frequency, srp_pump_rate_coefficient, srp_max_motor_speed, srp_shaft_diameter, pcp_pump_rate, pcp_rpm, pcp_screw_diameter, static_fluid_level, dynamic_fluid_level, responsible_person, phone_number) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35)';

            if (!Number(daily_well_parameters_entry_exists)) {
              await client.query(daily_well_parameters_insert_query, [
                well_id,
                report_date_id,
                flowmeter,
                well_uptime_hours,
                choke,
                pqa,
                phf,
                pba,
                p6x9,
                p9x13,
                p13x20,
                (gaslift_gas_day / 24) * well_uptime_hours,
                gaslift_system_pressure,
                pump_depth,
                pump_frequency,
                pump_hydrostatic_pressure,
                esp_pump_size,
                esp_pump_stages,
                esp_pump_rate,
                esp_pump_head,
                esp_downhole_gas_separator,
                srp_pumpjack_type,
                srp_pump_plunger_diameter,
                srp_plunger_stroke_length,
                srp_balancer_oscillation_frequency,
                srp_pump_rate_coefficient,
                srp_max_motor_speed,
                srp_shaft_diameter,
                pcp_pump_rate,
                pcp_rpm,
                pcp_screw_diameter,
                static_fluid_level,
                dynamic_fluid_level,
                responsible_person,
                phone_number,
              ]);
              logger.log(`|'daily_well_parameters'| Populated!`, success);
              daily_well_parameters_insertion_count++;
            } else {
              logger.log(`|'daily_well_parameters'| Already populated!`);
            }
            //
            ////

            //// populate well_tests table
            // check well_tests entry exists in DB
            const well_tests_entry_exists_query =
              'SELECT COUNT(*) AS well_tests_entry_exists FROM well_tests WHERE well_id = $1 AND well_test_date = $2';

            const { rows: well_tests_entry_exists_query_result } =
              await client.query(well_tests_entry_exists_query, [
                well_id,
                last_well_test_date_id,
              ]);

            const { well_tests_entry_exists } =
              well_tests_entry_exists_query_result[0] || {};
            //

            // insert entry into well_tests table
            const well_tests_insert_query =
              'INSERT INTO well_tests (well_id, report_date_id, well_test_date, liquid_ton, oil_ton, water_ton) VALUES ($1, $2, $3, $4, $5, $6)';

            if (!Number(well_tests_entry_exists)) {
              await client.query(well_tests_insert_query, [
                well_id,
                report_date_id,
                last_well_test_date_id,
                liquid_ton,
                oil_ton,
                water_ton,
              ]);
              logger.log(`|'well_tests'| Populated!`, success);
              well_tests_insertion_count++;
            } else {
              logger.log(`|'well_tests'| Already populated!`);
            }
            //
            ////

            //// populate gas_well_tests table
            // check gas_well_tests entry exists in DB
            const gas_well_tests_entry_exists_query =
              'SELECT COUNT(*) AS gas_well_tests_entry_exists FROM gas_well_tests WHERE well_id=$1 AND well_test_date=$2';

            const { rows: gas_well_tests_entry_exists_query_result } =
              await client.query(gas_well_tests_entry_exists_query, [
                well_id,
                last_gas_well_test_date_id,
              ]);

            const { gas_well_tests_entry_exists } =
              gas_well_tests_entry_exists_query_result[0] || {};
            //

            // insert entry into gas_well_tests table
            const gas_well_tests_insert_query =
              'INSERT INTO gas_well_tests (well_id, report_date_id, well_test_date, total_gas, gaslift_gas) VALUES ($1, $2, $3, $4, $5)';

            if (!Number(gas_well_tests_entry_exists)) {
              await client.query(gas_well_tests_insert_query, [
                well_id,
                report_date_id,
                field_id == 1
                  ? last_well_test_date_id
                  : last_gas_well_test_date_id,
                total_gas,
                gaslift_gas_wt,
              ]);
              logger.log(`|'gas_well_tests'| Populated!`, success);
              gas_well_tests_insertion_count++;
            } else {
              logger.log(`|'gas_well_tests'| Already populated!`);
            }
            //
            ////

            //// populate laboratory_results table
            // check laboratory_results entry exists in DB
            const laboratory_results_entry_exists_query =
              'SELECT COUNT(*) AS laboratory_results_entry_exists FROM laboratory_results WHERE well_id = $1 AND last_lab_date = $2';

            const { rows: laboratory_results_entry_exists_query_result } =
              await client.query(laboratory_results_entry_exists_query, [
                well_id,
                last_lab_date_id,
              ]);

            const { laboratory_results_entry_exists } =
              laboratory_results_entry_exists_query_result[0] || {};
            //

            // insert entry into laboratory_results table
            const laboratory_results_insert_query =
              'INSERT INTO laboratory_results (well_id, report_date_id, last_lab_date, water_cut, mechanical_impurities) VALUES ($1, $2, $3, $4, $5)';

            if (!Number(laboratory_results_entry_exists)) {
              await client.query(laboratory_results_insert_query, [
                well_id,
                report_date_id,
                last_lab_date_id,
                water_cut,
                mechanical_impurities,
              ]);
              logger.log(`|'laboratory_results'| Populated!`, success);
              laboratory_results_insertion_count++;
            } else {
              logger.log(`|'laboratory_results'| Already populated!`);
            }
            //
            ////

            // check whether well_test_date or last_lab_date is older than 15 days in producing wells
            if (
              production_well_stock_sub_category_id == 1 &&
              ((new Date(report_date) - new Date(last_well_test_date)) /
                86400000 >
                15 ||
                (new Date(report_date) - new Date(last_lab_date)) / 86400000 >
                  15)
            ) {
              logger.log(
                `Well test or lab results are too old for producing well`,
                warning
              );
            }
            //

            // check whether last_lab_date is obsolete
            // if (
            //   (new Date(report_date) - new Date(last_well_test_date)) / 86400000 >=
            //     7 &&
            //   last_well_test_date != last_lab_date
            // ) {
            //   logger.log(
            //     `Update lab results! Well test result should be available!`,
            //     warning
            //   );
            // }
            //

            // check whether lab results of well tests are present
            // const well_test_lab_result_not_exist_query =
            //   'SELECT TOP 10 well_test_date FROM well_tests AS wt ' +
            //   'WHERE well_id = @well_id ' +
            //   'AND (SELECT COUNT(*) = 0 FROM laboratory_results AS lr WHERE well_id = @well_id AND (DATEDIFF(day, wt.well_test_date, lr.last_lab_date) BETWEEN 0 AND 1)) ' +
            //   'AND DATEDIFF(day, CAST(GETDATE() AS DATE), wt.well_test_date) >= 7 ' +
            //   'ORDER BY wt.well_test_date DESC ';

            // const {recordset: well_test_lab_result_not_exist_list} = await pool
            //   .request()
            //   .input('well_id', well_id)
            //   .query(well_test_lab_result_not_exist_query);

            // if (well_test_lab_result_not_exist_list.length > 0) {
            //   const well_test_lab_result_not_exist_string =
            //     well_test_lab_result_not_exist_list
            //       .map((i) => i.well_test_date)
            //       .join(', ');
            //   logger.log(
            //     `Lab results of these well tests do not exist: ${well_test_lab_result_not_exist_string}`,
            //     warning
            //   );
            // }
            //
          }
          ////
        }
      }
    }
    ////
  }
  await processFiles(inputFolder);
} catch (err) {
  logger.log(err, error, false, true);
  console.log('Database error:', err);
} finally {
  client.release();
  logger.log(`${'='.repeat(100)}`, 'INFO', true);
  logger.log('SCRIPT EXECUTION FINISHED!', 'INFO', true);
  logger.log(`${'.'.repeat(100)}`, 'INFO', true);
  logger.log(`S U M M A R Y`, 'INFO', true);
  logger.log(
    `${success_count.toString().padStart(6)}\t\tsuccess`,
    'INFO',
    true
  );
  logger.log(`${error_count.toString().padStart(6)}\t\terror`, 'INFO', true);
  logger.log(
    `${warning_count.toString().padStart(6)}\t\twarning`,
    'INFO',
    true
  );
  logger.log(`${info_count.toString().padStart(6)}\t\tinfo`, 'INFO', true);
  logger.log(
    `\n${flowmeter_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'flowmeters'|`,
    'INFO',
    true
  );
  logger.log(
    `${well_stock_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'well_stock'|`,
    'INFO',
    true
  );
  logger.log(
    `${completion_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'completions'|`,
    'INFO',
    true
  );
  logger.log(
    `${well_tests_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'well_tests'|`,
    'INFO',
    true
  );
  logger.log(
    `${gas_well_tests_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'gas_well_tests'|`,
    'INFO',
    true
  );
  logger.log(
    `${daily_well_parameters_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'daily_well_parameters'|`,
    'INFO',
    true
  );
  logger.log(
    `${well_downtime_reasons_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'well_downtime_reasons'|`,
    'INFO',
    true
  );
  logger.log(
    `${laboratory_results_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'laboratory_results'|`,
    'INFO',
    true
  );
  logger.log(
    `${daily_general_comments_insertion_count
      .toString()
      .padStart(6)}\t\trow(s) inserted into |'daily_general_comments'|`,
    'INFO',
    true
  );
  logger.log(`${'.'.repeat(100)}`, 'INFO', true);
  logger.log(`${'*'.repeat(100)}`, 'INFO', true);
}

function getFormattedFilename(folder) {
  const now = new Date();
  const day = String(now.getDate()).padStart(2, '0');
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const year = now.getFullYear();

  return `./${folder}/${day}.${month}.${year}-dpr.log`;
}

//// when we insert into mysql date needs to be in specific format
function convertDateToMSsqlFormat(date) {
  const formattedDate = date
    .toLocaleString('en-US', {
      timeZone: 'Asia/Baku',
      hour12: false,
    })
    .split(',')[0];
  const [month, day, year] = formattedDate.split('/');
  const mysqlDate = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
  return mysqlDate;
}

//// sometimes we get date value as number (timestamp) when parsing excel file
function processDateValue(value) {
  if (typeof value === 'number') {
    const excelEpoch = new Date(Date.UTC(1899, 11, 30));
    const date = new Date(excelEpoch.getTime() + value * 86400000);
    return convertDateToMSsqlFormat(
      new Date(
        Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())
      )
    );
  } else if (typeof value === 'object' || value instanceof Date) {
    return convertDateToMSsqlFormat(value);
  }
  return convertDateToMSsqlFormat(value);
}

function isValidDate(value) {
  if (typeof value === 'number') {
    if (value >= 0 && value < 100) return true;
    return value > 18000;
  }
  return !isNaN(Date.parse(value));
}

function isYesterday(date1_id, date2_id) {
  return date1_id - date2_id == 1;
}
