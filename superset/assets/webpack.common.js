const fs = require('fs');
const path = require('path');
const webpack = require('webpack');
const CleanWebpackPlugin = require('clean-webpack-plugin');
const ManifestPlugin = require('webpack-manifest-plugin');
const ExtractTextPlugin = require('extract-text-webpack-plugin');
const WebpackAssetsManifest = require('webpack-assets-manifest');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');

const APP_DIR = path.resolve(__dirname, './'); // input dir
const BUILD_DIR = path.resolve(__dirname, './dist'); // output dir
const VERSION_STRING = JSON.parse(fs.readFileSync('package.json')).version;

module.exports = {
  node: {
    fs: 'empty',
  },
  entry: {
     home: APP_DIR + '/src/home.jsx',
     hdfsList: APP_DIR + '/src/hdfsList.js',

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
    chunkFilename: '[name].[chunkhash].chunk.js'
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
    // noParse: /mapbox-gl\/dist/,
    noParse: /(mapbox-gl)\.js$/,

    rules: [{
        test: /datatables\.net.*/,
        use: 'imports-loader?define=>false',
      },
      {
        test: /\.s?[ac]ss$/,
        include: APP_DIR,
        use: [
          "style-loader", // creates style nodes from JS strings
          "css-loader", // translates CSS into CommonJS
          "sass-loader" // compiles Sass to CSS
        ]
      },
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
    // new ManifestPlugin(),
    new WebpackAssetsManifest({
      publicPath: true,
      entrypoints: true, // this enables us to include all relevant files for an entry
    }),
    new CleanWebpackPlugin(['dist/']),

    /* new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: JSON.stringify(process.env.NODE_ENV),
      }
    }), */

    // new ExtractTextPlugin({
    //   filename: '[name].[chunkhash].entry.css',
    //   chunkFilename: '[name].[chunkhash].entry.css',
    // })
    new MiniCssExtractPlugin({
      filename: '[name].[chunkhash].entry.css',
      chunkFilename: '[name].[chunkhash].chunk.css',
    })
  ]
};