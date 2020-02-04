const parse = require('csv-parse');
const fs = require('fs');
const moment = require('moment');

const fileName = `node-data-processing-medium-data.csv`;
const fileFolder = `C:/Users/Vinoth/Downloads`;
const filePath = `${fileFolder}/${fileName}`;
const arrayData =[];

const config = {
  parserOptions: {
    columns: true,
    delimiter: ',',
    trim: true,
    skip_empty_lines: true,
    cast: true,
    cast_date: true,
  }
 };

const getFileStream = () => {
  return new Promise((resolve, reject) => {
    return resolve(fs.createReadStream(filePath));
  });
};

const processRow = (row) => {
    row['Order Month'] = row["Order Date"].getMonth() + 1;
    row['Order Year'] = row["Order Date"].getFullYear();
    arrayData.push(row); 
};

const recPriority = (acc,keyPriority) => {
    if(!acc.hasOwnProperty(keyPriority)){
        acc[keyPriority] =1;
    } else{
        acc[keyPriority]= acc[keyPriority] + 1;
    }
    return acc;
};

const computePriority =() => {
    const priority = arrayData.reduce((accumulator, row) => {   
        const keyYear = row['Order Year'];  
        const keyMonth = row['Order Month']; 
        const keyPriority = row['Order Priority'];

        
    if(!accumulator[keyYear]){
        accumulator[keyYear] = {};
    } 
    if (!accumulator[keyYear][keyMonth]){
        accumulator[keyYear][keyMonth] = {};
    } 
    accumulator[keyYear][keyMonth] = recPriority(accumulator[keyYear][keyMonth],keyPriority); 
   
    return accumulator;
  }, {});
  return priority;

}

const rollingAverage = (avg, sum, total) => {
    const currAvg = (sum + (avg * total)) / (total + 1);
    return Math.ceil(currAvg);
}


const computeAverageTime =() => {
    const avgTime = arrayData.reduce((accumulator, row) => {   
        const keyYear = row['Order Year'];  
        const keyMonth = row['Order Month']; 
        const keyRegion = row['Region'];
        const keyCountry = row['Country'];
    if(!accumulator[keyYear]){
        accumulator[keyYear] = {};
    } 
    if (!accumulator[keyYear][keyMonth]){
        accumulator[keyYear][keyMonth] = {};
    }

    if(!accumulator[keyYear][keyMonth]['AvgDaysToShip']){
        accumulator[keyYear][keyMonth]['AvgDaysToShip'] = 0;
    }
    if(!accumulator[keyYear][keyMonth]['NumberOfOrders']){
        accumulator[keyYear][keyMonth]['NumberOfOrders'] =0;
    }
    if (!accumulator[keyYear][keyMonth]['Regions']){
        accumulator[keyYear][keyMonth]['Regions'] = {};
    }
    if(!accumulator[keyYear][keyMonth]['Regions'][keyRegion]){
        accumulator[keyYear][keyMonth]['Regions'][keyRegion] = {};
    }
    if(!accumulator[keyYear][keyMonth]['Regions'][keyRegion]['AvgDaysToShip']){
        accumulator[keyYear][keyMonth]['Regions'][keyRegion]['AvgDaysToShip'] = 0;
    }
    if(!accumulator[keyYear][keyMonth]['Regions'][keyRegion]['NumberOfOrders']){
        accumulator[keyYear][keyMonth]['Regions'][keyRegion]['NumberOfOrders'] =0;
    }
    if (!accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries']){
        accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'] = {};
    }
    if (!accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]){
        accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry] = {};
    }
    if(!accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]['AvgDaysToShip']){
        accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]['AvgDaysToShip'] = 0;
    }
    if(!accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]['NumberOfOrders']){
        accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]['NumberOfOrders'] =0;
    }


    
    const daysToShip = moment(row['Ship Date'],'YYYY-MM-DD').diff(moment(row['Order Date'],'YYYY-MM-DD'),'days');

    accumulator[keyYear][keyMonth]['AvgDaysToShip'] = rollingAverage(accumulator[keyYear][keyMonth]['AvgDaysToShip'], daysToShip,accumulator[keyYear][keyMonth]['NumberOfOrders']);
    accumulator[keyYear][keyMonth]['NumberOfOrders'] ++;

    accumulator[keyYear][keyMonth]['Regions'][keyRegion]['AvgDaysToShip'] = rollingAverage(accumulator[keyYear][keyMonth]['Regions'][keyRegion]['AvgDaysToShip'], daysToShip,accumulator[keyYear][keyMonth]['Regions'][keyRegion]['NumberOfOrders']);
    accumulator[keyYear][keyMonth]['Regions'][keyRegion]['NumberOfOrders'] ++;

    accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]['AvgDaysToShip'] = rollingAverage(accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]['AvgDaysToShip'], daysToShip,accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]['NumberOfOrders']);
    accumulator[keyYear][keyMonth]['Regions'][keyRegion]['Countries'][keyCountry]['NumberOfOrders'] ++;

    return accumulator;
  }, {});
  return avgTime;
}

const buildNesting = (obj, path, value) => {
     if(!obj[path[0]]){
        obj[path[0]] = {};
    } 
    if (path.length === 1) {
      obj[path] = value;
      return;
    }
    buildNesting(obj[path[0]], path.slice(1), value);
    return obj;
}

const computeRevenue =() => {
    const revAggregate = arrayData.reduce((accumulator, row) => {   
      
        const keyRegion = row['Region'];  
        const keyItemType = row['Item Type']; 
        const keyCountry = row['Country'];
        
        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Total','revenue'],row['Total Revenue']);
        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Total','cost'],row['Total Cost']);
        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Total','profit'],row['Total Profit']);

        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Countries',keyCountry,'Total','revenue'],row['Total Revenue']);
        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Countries',keyCountry,'Total','cost'],row['Total Cost']);
        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Countries',keyCountry,'Total','profit'],row['Total Profit']);

        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Countries',keyCountry,'ItemTypes',keyItemType,'revenue'],row['Total Revenue']);
        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Countries',keyCountry,'ItemTypes',keyItemType,'cost'],row['Total Cost']);
        accumulator = buildNesting(accumulator, ['Regions',keyRegion,'Countries',keyCountry,'ItemTypes',keyItemType,'profit'],row['Total Profit']);

        accumulator = buildNesting(accumulator, ['ItemTypes',keyItemType,'revenue'],row['Total Revenue']);
        accumulator = buildNesting(accumulator, ['ItemTypes',keyItemType,'cost'],row['Total Cost']);
        accumulator = buildNesting(accumulator, ['ItemTypes',keyItemType,'profit'],row['Total Profit']);
 
    return accumulator;
  }, {});

  return revAggregate;

}


const generateJSONFiles =() => {
    const priorityTask = computePriority();
    fs.writeFileSync('priority.json',JSON.stringify(priorityTask), 'utf8');
    const avgTimeTask = computeAverageTime();
    fs.writeFileSync('average.json',JSON.stringify(avgTimeTask), 'utf8'); 
    const revenueTask = computeRevenue();
    fs.writeFileSync('revenue.json',JSON.stringify(revenueTask), 'utf8');
}

const parseFile = () => {
  getFileStream().then(stream => {
    stream.pipe(parse(config.parserOptions))
      .on('data', csvrow => processRow(csvrow)) 
      .on('error', error => console.log(error))
      .on('end', () => generateJSONFiles())
  });
};

parseFile();
  