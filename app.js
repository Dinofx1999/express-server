var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
const cors = require('cors');
var indexRouter = require('./routes/Controller/index');
var usersRouter = require('./routes/users');

var Login = require('./routes/Controller/auth.js');
const symbolConfig = require('./routes/Controller/symbolConfig.js');
const ErrorAnalysis = require('./routes/Controller/errors.js');
const {  API_ALL_INFO_BROKERS , 
          VERSION,
          API_PORT_BROKER_ENDPOINT, 
          API_RESET , 
          API_RESET_ALL_ONLY_SYMBOL ,
          API_CONFIG_SYMBOL , 
          API_ANALYSIS_CONFIG , API_PRICE_SYMBOL ,
          API_RESET_ALL_BROKERS , API_GET_CONFIG_SYMBOL } = require('./routes/Module/Constants/API.Service.js');



var app = express();




app.use(cors({
  origin: '*',
}));


// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use(``, indexRouter);
app.use(`/${VERSION}/users`, usersRouter);

app.use(`/auth`, Login);
app.use(`/${VERSION}/symbol`, symbolConfig);
app.use(`/${VERSION}/errors`, ErrorAnalysis);


// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
