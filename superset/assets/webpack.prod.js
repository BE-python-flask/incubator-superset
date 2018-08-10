const webpack = require('webpack');
const path = require('path');
const MiniCssExtractPlugin = require("mini-css-extract-plugin");

const merge = require('webpack-merge');
const common = require('./webpack.common.js');
const APP_DIR = path.resolve(__dirname, './'); // input dir

module.exports = merge(common, {
    mode: 'production',
    plugins: [
        new webpack.DefinePlugin({
            'process.env.NODE_ENV': JSON.stringify('production')
        }),
        new MiniCssExtractPlugin({
            filename: '[name].[chunkhash].entry.css',
            chunkFilename: '[name].[chunkhash].chunk.css'
        })
    ],
    optimization: {
        // noEmitOnErrors: true
    },
    module: {
        rules: [{
            test: /\.css$/,
            include: APP_DIR,
            use: [MiniCssExtractPlugin.loader, 'css-loader']
        },{
            test: /\.(scss|sass)$/,
            use: [
                MiniCssExtractPlugin.loader, // creates style nodes from JS strings
                "css-loader", // translates CSS into CommonJS
                "sass-loader" // compiles Sass to CSS
            ]
        },{
            test: /\.less$/,
            include: APP_DIR,
            use: [
                MiniCssExtractPlugin.loader,
                'css-loader',
                'less-loader'
            ]
        }]
    }
});