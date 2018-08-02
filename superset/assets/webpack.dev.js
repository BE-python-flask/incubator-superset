const path = require('path');
const webpack = require('webpack');
const merge = require('webpack-merge');
const MiniCssExtractPlugin = require("mini-css-extract-plugin");

const common = require('./webpack.common.js');
const APP_DIR = path.resolve(__dirname, './'); // input dir

module.exports = merge(common, {
    mode: 'development',
    devtool: 'cheap-module-eval-source-map',
    // devtool: 'inline-source-map',
    // devtool: 'source-map',
    plugins: [
        new webpack.DefinePlugin({
            'process.env.NODE_ENV': JSON.stringify('development')
        }),
        new MiniCssExtractPlugin({
            filename: '[name].[chunkhash].entry.css',
            chunkFilename: '[name].[chunkhash].chunk.css'
        })
    ],
    module: {
        rules: [
            {
            test: /\.css$/,
            include: APP_DIR,
            use: [MiniCssExtractPlugin.loader, 'css-loader']
          },
          {
            test: /\.less$/,
            include: APP_DIR,
            use: [
                MiniCssExtractPlugin.loader,
                'css-loader',
                'less-loader'
            ]
          },
        ]
    }
});