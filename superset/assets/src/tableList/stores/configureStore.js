import { createStore, applyMiddleware, compose } from 'redux';
import thunkMiddleware from 'redux-thunk';
import {createLogger} from 'redux-logger';
import rootReducer from '../reducers';
import {persistStore, autoRehydrate} from 'redux-persist';
import { asyncSessionStorage } from 'redux-persist/storages';

export default function configureStore(initialState) {
  const store = createStore(
    rootReducer,
    initialState,
    applyMiddleware(thunkMiddleware, createLogger()),
    compose(
        applyMiddleware(thunkMiddleware, createLogger()),
        autoRehydrate(),
        window.devToolsExtension ? window.devToolsExtension() : f => f
    )
  );

  persistStore(store, {storage: asyncSessionStorage, blacklist: ['routing']});

  if (module.hot) {
    // Enable Webpack hot module replacement for reducers
    module.hot.accept('../reducers', () => {
      const nextRootReducer = require('../reducers').default;
      store.replaceReducer(nextRootReducer);
    });
  }

  return store;
}
