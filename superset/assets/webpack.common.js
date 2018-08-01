const fs = require('fs');
const path = require('path');
const webpack = require('webpack');
const CleanWebpackPlugin = require('clean-webpack-plugin');
const ManifestPlugin = require('webpack-manifest-plugin');
const ExtractTextPlugin = require('extract-text-webpack-plugin');

const APP_DIR = path.resolve(__dirname, './'); // input dir
const BUILD_DIR = path.resolve(__dirname, './dist'); // output dir
const VERSION_STRING = JSON.parse(fs.readFileSync('package.json')).version;

module.exports = {
  node: {
    fs: 'empty',
  },
  target: 'web',
  entry: {
    // 'css-theme': APP_DIR + '/javascripts/css-theme.js',
    // user: APP_DIR + '/javascripts/user.js',
    home: APP_DIR + '/src/home.jsx',
    // sliceList: APP_DIR + '/javascripts/sliceList.js',
    // databaseList: APP_DIR + '/javascripts/databaseList.js',
    // tableList: APP_DIR + '/javascripts/tableList.js',
    // hdfsList: APP_DIR + '/javascripts/hdfsList.js',
    // dashboardEntry: APP_DIR + '/javascripts/dashboardEntry.js',

    // common: APP_DIR + '/javascripts/common.js',
    // dashboard: APP_DIR + '/javascripts/dashboard/Dashboard.jsx',
    // explore: APP_DIR + '/javascripts/explore/explore.jsx',
    // explorev2: APP_DIR + '/javascripts/explorev2/index.jsx',
    // sqllab: APP_DIR + '/javascripts/SqlLab/index.jsx',
    // standalone: APP_DIR + '/javascripts/standalone.js', // TODO:
    // welcome: APP_DIR + '/javascripts/welcome.js',
    // profile: APP_DIR + '/javascripts/profile/index.jsx'

    theme: APP_DIR + '/src/theme.js',
    common: APP_DIR + '/src/common.js',
    addSlice: APP_DIR + '/src/addSlice/index.jsx',
    explore: APP_DIR + '/src/explore/index.jsx',
    dashboard: APP_DIR + '/src/dashboard/index.jsx',
    dashboard_deprecated: APP_DIR + '/src/dashboard/deprecated/v1/index.jsx',
    sqllab: APP_DIR + '/src/SqlLab/index.jsx',
    welcome: APP_DIR + '/src/welcome/index.jsx',
    profile: APP_DIR + '/src/profile/index.jsx'
  },
  output: {
    path: BUILD_DIR,
    // filename: `[name].${VERSION_STRING}.entry.js`,
    filename: '[name].[chunkhash].entry.js',
    chunkFilename: '[name].[chunkhash].entry.js'
  },
  resolve: {
    extensions: ['.js', '.jsx', '.ts', '.tsx', '.css', '.sass', '.scss', '.less'],
    // alias: {
    //     'mapbox-gl/js/geo/transform': path.join(
    //         __dirname, '/node_modules/mapbox-gl/js/geo/transform'),
    //     'mapbox-gl': path.join(__dirname, '/node_modules/mapbox-gl/js/mapbox-gl.js')
    // }
  },
  module: {
    noParse: /mapbox-gl\/dist/,
    rules: [{
        test: /datatables\.net.*/,
        use: 'imports-loader?define=>false',
      },
      // style
      {
        test: /\.s?[ac]ss$/,
        include: APP_DIR,
        use: [
          "style-loader", // creates style nodes from JS strings
          "css-loader", // translates CSS into CommonJS
          "sass-loader" // compiles Sass to CSS
        ]
      },
      /*{
          test: /\.less$/,
          include: APP_DIR,
          use: [{
              loader: 'style-loader'
          }, {
              loader: 'css-loader'
          }, {
              loader: 'less-loader',
              options: {
                  strictMath: true,
                  noIeCompat: true
              }
          }]
      }, */
      {
        test: /\.less$/,
        use: [{
          loader: 'style-loader'
        }, {
          loader: 'css-loader'
        }, {
          loader: 'less-loader'
        }]
      },
      // {
      //   test: /\.less$/,
      //   include: APP_DIR,
      //   loader: ExtractTextPlugin.extract({
      //     use: ['css-loader', 'less-loader'],
      //     fallback: 'style-loader',
      //   }),
      // },

      // images
      /* for css linking images */
      {
        test: /\.png$/,
        use: {
          loader: 'url-loader?limit=1024&name=[path][name].[hash:8].[ext]',
          options: {
            limit: 20000
          }
        }
      },
      {
        test: /\.(svg|jpg|gif)$/,
        use: ['file-loader'],
      },
      /* for mapbox */
      {
        test: /\.js$/,
        include: APP_DIR + '/node_modules/mapbox-gl/js/render/painter/use_program.js',
        use: {
          loader: 'transform/cacheable?brfs'
        }
      },
      {
        test: /\.(js|jsx)$/,
        exclude: /(node_modules|bower_components)/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['babel-preset-airbnb', 'babel-preset-env', 'babel-preset-react', 'babel-preset-stage-0']
          }
        }
      },

      /* for react-map-gl overlays */
      {
        test: /\.react\.js$/,
        include: APP_DIR + '/node_modules/react-map-gl/src/overlays',
        use: ['babel-loader'],
        enforce: 'post'
      },
      {
        test: /\.(ts|tsx)$/,
        use: 'ts-loader',
        exclude: /node_modules/
      },

      /* for font-awesome */
      {
        test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        use: 'url-loader?limit=10000&minetype=application/font-woff'
      },
      {
        test: /\.(ttf|eot|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        use: 'file-loader'
      }
    ]
  },
  externals: {
    cheerio: 'window',
    'react/lib/ExecutionEnvironment': true,
    'react/lib/ReactContext': true,
    xmlhttprequest: '{XMLHttpRequest:XMLHttpRequest}'
  },
  plugins: [
    new ManifestPlugin(),
    new CleanWebpackPlugin(['dist/']),
    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: JSON.stringify(process.env.NODE_ENV),
      }
    }),
    new ExtractTextPlugin('[name].[chunkhash].css')
  ]
};