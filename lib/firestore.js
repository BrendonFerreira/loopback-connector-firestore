const admin = require('firebase-admin');
const util = require('util');
const Connector = require('loopback-connector').Connector;

exports.initialize = function initializeDataSource(dataSource, callback) {
	dataSource.connector = new Firestore(dataSource.settings);
	process.nextTick(() => {
		callback();
	});
};

class Firestore {
	constructor(dataSourceProps) {
		this._models = {};

		admin.initializeApp({
			credential: admin.credential.cert({
				projectId: dataSourceProps.projectId,
				clientEmail: dataSourceProps.clientEmail,
				privateKey: dataSourceProps.privateKey.replace(/\\n/g, '\n'),
			}),
			databaseURL: `https://${dataSourceProps.databaseName}` || `${dataSourceProps.projectId}.firebaseio.com`,
		});

		this.db = admin.firestore();
	}

	/**
     * Find matching model instances by the filter
     *
     * @param {String} model The model name
     * @param {Object} filter The filter
     * @param {Function} [callback] The callback function
     */
	all(model, filter, callback) {
		const data = [];
		const collection = this.db.collection(model)
		if (filter && filter.where ) {
			if( filter.where.id ) {
				collection.doc(filter.where.id).get().then(doc => {
					if (!doc.exists) {
						callback(null, {});
					} else {
						const docContent = doc.data();
						docContent.id = doc.id;
						callback(null, [docContent]);
					}
				}).catch(err => callback(err));
			} else {
				
				this.query( model, filter ).get().then(snapshot => {
					snapshot.forEach(doc => {
						const docu = doc.data();
						docu.id = doc.id;
						data.push(docu);
					});
				}).then(() => callback(null, data)).catch(err => callback(err));
			}
		} else { // GET all operation
			collection.get().then(snapshot => {
				snapshot.forEach(doc => {
					const docu = doc.data();
					docu.id = doc.id;
					data.push(docu);
				});
			}).then(() => callback(null, data)).catch(err => callback(err));
		}
	}

	query( model, filter ) {
		
		const collection = this.db.collection(model)
		
		let filteredCollection = collection
		
		for( let key in filter.where ) {
			if( ![ 'eq', 'lt', 'lte', 'bt', 'bte' ].includes( key ) ){
				filteredCollection = filteredCollection.where( key, '==', filter.where[key] )
			}
		}

		if( filter.orderBy ) {
			filteredCollection = filteredCollection.orderBy( filter.orderBy )
		}

		return filteredCollection
	}

	create(model, data, callback) {
		this.db.collection(model).add(data).then(ref => callback(null, ref.id)).catch(err => callback(err));
	}

	/**
     * Update all matching instances
     * @param {String} model The model name
     * @param {Object} where The search criteria
     * @param {Object} data The property/value pairs to be updated
     * @callback {Function} callback Callback function
     */
	update(model, filter, data, options, callback) {
		const self = this;
		this.query(model, filter).limit(1).update(data)
		.then((result) => {
			callback( null, result )
		})
		.catch(callback)
	}

	updateAll(model, filter, data, options, callback) {
		const self = this;
		this.query(model, filter).update(data)
		.then((result) => {
			callback( null, result )
		})
		.catch(callback)
	}

	/**
     * Replace properties for the model instance data
     * @param {String} model The name of the model
     * @param {*} id The instance id
     * @param {Object} data The model data
     * @param {Object} options The options object
     * @param {Function} [callback] The callback function
     */
	replaceById(model, id, data, options, callback) {
		const self = this;
		this.exists(model, id, null, (err, res) => {
			if (err) callback(err);
			if (res) {
				self.db.collection(model).doc(id).update(data).then(() => {
					// Document updated successfully.
					callback(null, []);
				});
			} else {
				callback('Document not found');
			}
		});
	}

	/**
     * Update properties for the model instance data
     * @param {String} model The model name
     * @param {*} id The instance id
     * @param {Object} data The model data
     * @param {Function} [callback] The callback function
     */
	updateAttributes(model, id, data, options, callback) {
		const self = this;
		this.exists(model, id, null, (err, res) => {
			if (err) callback(err);
			if (res) {
				self.db.collection(model).doc(id).set(data).then(() => {
					// Document updated successfully.
					callback(null, []);
				});
			} else {
				callback('Document not found');
			}
		});
	}

	/**
     * Delete a model instance by id
     * @param {String} model The model name
     * @param {*} id The instance id
     * @param [callback] The callback function
     */
	destroyById(model, id, callback) {
		const self = this;
		this.exists(model, id, null, (err, res) => {
			if (err) callback(err);
			if (res) {
				self.db.collection(model).doc(id).delete().then(() => {
					// Document deleted successfully.
					callback(null, []);
				});
			} else {
				callback('Document not found');
			}
		});
	}

	/**
     * Delete a model instance
     * @param {String} model The model name
     * @param {Object} where The id Object
     * @param [callback] The callback function
     */
	destroyAll(model, where, callback) {
		const self = this;

		if (where.id) {
			this.exists(model, where.id, null, (err, res) => {
				if (err) callback(err);
				if (res) {
					self.db.collection(model).doc(where.id).delete().then(() => {
						// Document deleted successfully.
						callback(null, []);
					});
				} else {
					callback('Document not found');
				}
			});
		} else {
			this.deleteCollection(this.db, model, 10).then(() => {
				callback(null, '');
			}).catch(err => callback(err));
		}
	}

	deleteQueryBatch(db, query, batchSize, resolve, reject) {
		query.get()
			.then((snapshot) => {
				// When there are no documents left, we are done
				if (snapshot.size == 0) {
					return 0;
				}

				// Delete documents in a batch
				const batch = db.batch();
				snapshot.docs.forEach(doc => batch.delete(doc.ref));

				return batch.commit().then(() => snapshot.size);
			}).then(numDeleted => {
				if (numDeleted === 0) {
					resolve();
					return;
				}

				// Recurse on the next process tick, to avoid
				// exploding the stack.
				process.nextTick(() => {
					this.deleteQueryBatch(db, query, batchSize, resolve, reject);
				});
			})
			.catch(reject);
	}

	deleteCollection(db, collectionPath, batchSize) {
		const collectionRef = db.collection(collectionPath);
		const query = collectionRef.orderBy('__name__').limit(batchSize);

		return new Promise((resolve, reject) => {
			this.deleteQueryBatch(db, query, batchSize, resolve, reject);
		});
	}

	/**
     * Check if a model instance exists by id
     * @param {String} model The model name
     * @param {*} id The id value
     * @param {Function} [callback] The callback function
     *
     */
	exists(model, id, options, callback) {
		this.db.collection(model).doc(id).get().then(doc => {
			callback(null, doc.exists);
		}).catch(err => callback(err));
	}

	/**
     * Count the number of instances for the given model
     *
     * @param {String} model The model name
     * @param {Object} where The id Object
     * @param {Function} [callback] The callback function
     *
     */
	count(model, where, options, callback) {
		if (Object.keys(where).length > 0) {
			this.db.collection(model).where(Object.keys(where)[0], '==', Object.values(where)[0]).get().then(doc => {
				callback(null, doc.docs.length);
			}).catch(err => callback(err));
		} else {
			this.db.collection(model).get().then(doc => {
				callback(null, doc.docs.length);
			}).catch(err => callback(err));
		}
	}

	ping(callback) {
		if (this.db.projectId) {
			callback(null);
		} else {
			callback('Ping Error');
		}
	}
}

util.inherits(Firestore, Connector);
exports.Firestore = Firestore;
