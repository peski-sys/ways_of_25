if (ATRAD == undefined) {
    var ATRAD = {};
}
ATRAD.AJAXConnection = function(a, d, e, c, b) {
    this.URL = a;
    this.callbackFun = d;
    this.handleAs = e;
    this.timeOut = 0;
    this.errorIns = b;
    this.reqCompleted = "false";
    this.reqSuccess = "false";
    this.langTranslator = new ATRAD.ClientMsgTranslator();
}
;
ATRAD.AJAXConnection = function(a, e, d, c, b, h, f) {
    this.URL = a;
    this.callbackFun = e;
    this.handleAs = d;
    this.timeOut = 0;
    this.errorIns = b;
    this.reqCompleted = "false";
    this.reqSuccess = "false";
    this.langTranslator = new ATRAD.ClientMsgTranslator();
    this.errorOccureCallaback = h;
    this.atradForm = f;
    var g;
    this.errUrl = "";
    this.errTime = "";
}
;
ATRAD.AJAXConnection.instanceCount = 0;
ATRAD.AJAXConnection.prototype = {
    name: null,
    URL: null,
    callbackFun: null,
    handleAs: null,
    timeOut: null,
    errorIns: null,
    reqCompleted: null,
    reqSuccess: null,
    langTranslator: null,
    errorOccureCallaback: null,
    atradForm: null,
    RequestData: function() {
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        }
        this.reqCompleted = "false";
        this.reqSuccess = "false";
        if (this.timeoutValue > 0) {
            dojo.xhrGet({
                preventCache: true,
                url: this.URL,
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handleError),
                handleAs: this.handleAs,
                timeout: this.timeOut
            });
        } else {
            dojo.xhrGet({
                preventCache: true,
                url: this.URL,
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handleError),
                handleAs: this.handleAs
            });
        }
    },
    RequestBuySellData: function() {
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        }
        this.reqCompleted = "false";
        this.reqSuccess = "false";
        if (this.timeoutValue > 0) {
            dojo.xhrGet({
                preventCache: true,
                url: this.URL,
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handleBuySellError),
                handleAs: this.handleAs,
                timeout: this.timeOut
            });
        } else {
            dojo.xhrGet({
                preventCache: true,
                url: this.URL,
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handleBuySellError),
                handleAs: this.handleAs
            });
        }
    },
    SubmitData: function() {
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        }
        this.reqCompleted = "false";
        this.reqSuccess = "false";
        if (this.timeoutValue > 0) {
            dojo.xhrPost({
                form: this.atradForm,
                handleAs: this.handleAs,
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handleError),
                timeout: this.timeOut
            });
        } else {
            dojo.xhrPost({
                form: this.atradForm,
                handleAs: "json",
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handleError)
            });
        }
    },
    SubmitBuySellData: function() {
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        }
        this.reqCompleted = "false";
        this.reqSuccess = "false";
        if (this.timeoutValue > 0) {
            dojo.xhrPost({
                form: this.atradForm,
                handleAs: this.handleAs,
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handleBuySellError),
                timeout: this.timeOut
            });
        } else {
            dojo.xhrPost({
                form: this.atradForm,
                handleAs: "json",
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handleBuySellError)
            });
        }
    },
    RequestLoginPageData: function() {
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        }
        this.reqCompleted = "false";
        this.reqSuccess = "false";
        dojo.xhrGet({
            preventCache: true,
            url: this.URL,
            load: this.bind(this, this.logincallback),
            error: this.bind(this, this.handleLoginError),
            handleAs: this.handleAs
        });
    },
    SubmitLoginData: function() {
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        }
        this.reqCompleted = "false";
        this.reqSuccess = "false";
        if (this.timeoutValue > 0) {
            dojo.xhrPost({
                form: this.atradForm,
                handleAs: this.handleAs,
                load: this.bind(this, this.logincallback),
                error: this.bind(this, this.handleLoginError),
                timeout: this.timeOut
            });
        } else {
            dojo.xhrPost({
                form: this.atradForm,
                handleAs: "json",
                load: this.bind(this, this.logincallback),
                error: this.bind(this, this.handleLoginError)
            });
        }
    },
    logincallback: function(a) {
        if (this.URL == this.errUrl) {
            this.errUrl = "";
        }
        errorTime = undefined;
        this.reqSuccess = "true";
        this.reqCompleted = "true";
        if (this.handleAs == "json") {
            this.processLoginJSONResponce(a);
        }
        if (this.handleAs == "text") {
            this.processTextResponce(a);
        }
    },
    handleLoginError: function(a) {
        this.reqCompleted = "true";
        this.reqSucces = "false";
        if (a.status == 0) {
            this.errorIns.setContentMessage("Connection error please check your connection");
        } else {
            if (a.status == 404) {
                this.errorIns.setContentMessage("Error while loading data");
            } else {
                if (a.type = "unexpected_token") {
                    this.errorIns.innerHTML = "<strong>Your session is invalidated</strong>";
                } else {
                    this.errorIns.innerHTML = "<strong>Error while loading data</strong>";
                }
            }
        }
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        } else {
            if (a.status == 0 || a.type == "unexpected_token") {
                ATRAD.AJAXConnection.instanceCount++;
            }
            if (this.errorOccureCallaback != null) {
                this.errorOccureCallaback(a);
            }
        }
    },
    processLoginJSONResponce: function(b) {
        var c = b.code;
        var a = b.description;
        if (c != 0) {
            if (this.errorOccureCallaback != null) {
                this.errorOccureCallaback(b);
            }
        } else {
            this.callbackFun(b);
        }
    },
    handleError: function(b) {
        this.reqCompleted = "true";
        this.reqSucces = "false";
        if (b.status == 0) {
            this.errorIns.setContentMessage("Connection error please check your connection");
        } else {
            if (b.status == 404) {
                this.errorIns.setContentMessage("Error while loading data");
            } else {
                if (b.type = "unexpected_token") {
                    this.errorIns.setContentMessage("Your session is invalidated. Please login to continue");
                } else {
                    this.errorIns.setContentMessage("Error while loading data");
                }
            }
        }
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        } else {
            this.errorIns.show();
            if (b.status == 0 || b.type == "unexpected_token") {
                ATRAD.AJAXConnection.instanceCount++;
                var a = this.errorIns.errorWindow.getChildren();
                dojo.connect(a[0], "onClick", function() {
                    document.location.reload();
                });
            }
            if (this.errorOccureCallaback != null) {
                this.errorOccureCallaback(b);
            }
        }
    },
    handleBuySellError: function(a) {
        this.reqCompleted = "true";
        this.reqSucces = "false";
        if (a.status == 0) {
            this.errorIns.setContentMessage("Connection error please check your connection");
        } else {
            if (a.status == 404) {
                this.errorIns.setContentMessage("Error while loading data");
            } else {
                if (a.type = "unexpected_token") {
                    this.errorIns.setContentMessage("Your session is invalidated. Please login to continue");
                } else {
                    this.errorIns.setContentMessage("Error while loading data");
                }
            }
        }
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        } else {
            if (a.status == 0 || a.type == "unexpected_token") {
                ATRAD.AJAXConnection.instanceCount++;
            }
            if (this.errorOccureCallaback != null) {
                this.errorOccureCallaback(a);
            }
        }
    },
    RequestPoolData: function() {
        if (ATRAD.AJAXConnection.instanceCount > 0) {
            return;
        }
        this.reqCompleted = "false";
        this.reqSuccess = "false";
        if (this.timeoutValue > 0) {
            dojo.xhrGet({
                preventCache: true,
                url: this.URL,
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handlePoolError),
                handleAs: this.handleAs,
                timeout: this.timeOut
            });
        } else {
            dojo.xhrGet({
                preventCache: true,
                url: this.URL,
                load: this.bind(this, this.callback),
                error: this.bind(this, this.handlePoolError),
                handleAs: this.handleAs
            });
        }
    },
    handlePoolError: function(b) {
        this.reqCompleted = "true";
        this.reqSucces = "false";
        var e = new Date();
        if (errorTime) {
            var c = e.getTime();
            if ((c - errorTime) > 60000) {
                if (b.status == 0) {
                    this.errorIns.setContentMessage("Connection error please check your connection");
                } else {
                    if (b.status == 404) {
                        this.errorIns.setContentMessage("Error while loading data");
                    } else {
                        if (b.type = "unexpected_token") {
                            this.errorIns.setContentMessage("Your session is invalidated. Please login to continue");
                        } else {
                            this.errorIns.setContentMessage("Error while loading data");
                        }
                    }
                }
                if (ATRAD.AJAXConnection.instanceCount > 0) {
                    return;
                } else {
                    this.errorIns.show();
                    if (b.status == 0 || b.type == "unexpected_token") {
                        ATRAD.AJAXConnection.instanceCount++;
                        var a = this.errorIns.errorWindow.getChildren();
                        dojo.connect(a[0], "onClick", function() {
                            document.location.reload();
                        });
                    }
                    if (this.errorOccureCallaback != null) {
                        this.errorOccureCallaback(b);
                    }
                }
            }
        } else {
            if (this.errUrl == this.URL) {
                var c = e.getTime();
                var f = c - this.errTime;
                if (f && f > 60000) {
                    if (b.status == 0) {
                        this.errorIns.setContentMessage("Connection error please check your connection");
                    } else {
                        if (b.status == 404) {
                            this.errorIns.setContentMessage("Error while loading data");
                        } else {
                            if (b.type = "unexpected_token") {
                                this.errorIns.setContentMessage("Your session is invalidated. Please login to continue");
                            } else {
                                this.errorIns.setContentMessage("Error while loading data");
                            }
                        }
                    }
                    if (ATRAD.AJAXConnection.instanceCount > 0) {
                        return;
                    } else {
                        this.errorIns.show();
                        if (b.status == 0 || b.type == "unexpected_token") {
                            ATRAD.AJAXConnection.instanceCount++;
                            var a = this.errorIns.errorWindow.getChildren();
                            dojo.connect(a[0], "onClick", function() {
                                document.location.reload();
                            });
                        }
                        if (this.errorOccureCallaback != null) {
                            this.errorOccureCallaback(b);
                        }
                    }
                }
            } else {
                errorTime = e.getTime();
                this.errUrl = this.URL;
                this.errTime = e.getTime();
            }
        }
    },
    bind: function(b, a) {
        return function() {
            a.apply(b, arguments);
        }
        ;
    },
    callback: function(a) {
        if (this.URL == this.errUrl) {
            this.errUrl = "";
        }
        errorTime = undefined;
        this.reqSuccess = "true";
        this.reqCompleted = "true";
        if (this.handleAs == "json") {
            this.processJSONResponce(a);
        }
        if (this.handleAs == "text") {
            this.processTextResponce(a);
        } else {}
    },
    isRequestCompleted: function() {
        return this.reqCompleted;
    },
    isRequestSuccess: function() {
        return this.reqSuccess;
    },
    processJSONResponce: function(b) {
        var c = b.code;
        var a = b.description;
        if (c != 0) {
            this.errorIns.setContentMessage(this.langTranslator.getString(a));
            this.errorIns.show();
            if (this.errorOccureCallaback != null) {
                this.errorOccureCallaback(b);
            }
        } else {
            this.callbackFun(b);
        }
    },
    processTextResponce: function(a) {
        this.callbackFun(a);
    },
    setURL: function(a) {
        this.URL = a;
    }
};
