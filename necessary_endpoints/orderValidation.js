if (ATRAD == undefined) {
    var ATRAD = {};
}
ATRAD.OrderValidation = (function() {
    var orderWin_ = null;
    var showError = function(error, role, show, aprovalEnable) {
        var message = language.getString(error);
        if (typeof show !== "undefined") {
            message = message + " " + show;
        }
        if (typeof role !== "undefined" && role == "Manager") {
            var callBack = confirmationCallBack;
            var confBox = new ATRAD.ConfirmationMessage();
            confBox.setTitle("Order");
            confBox.setHeight("100px");
            confBox.setMessage(message + "<br/> Do you need to procced the order ?");
            confBox.setCallback(callBack);
            confBox.show();
            return false;
        } else {
            if (typeof role !== "undefined" && (role == "SuperAdvisor" || role == "Advisor") && aprovalEnable == "1") {
                var callBack = confirmationCallBack;
                var confBox = new ATRAD.ConfirmationMessage();
                confBox.setTitle("Order");
                confBox.setHeight("100px");
                confBox.setMessage(message + "<br/> Do you want to proceed the order for approval ?");
                confBox.setCallback(callBack);
                confBox.show();
            } else {
                var errorDisplay = new ATRAD.ErrorPanel();
                errorDisplay.setType(1);
                errorDisplay.setContentMessage(message);
                errorDisplay.setHeight("120px");
                errorDisplay.setLength("400px");
                errorDisplay.show();
                return false;
            }
        }
    };
    var confirmationCallBack = function(response) {
        if (response == "yes") {
            orderWin_.buyDisableValidate(true);
        } else {
            orderWin_.prototype.buyDisableValidate(false);
        }
    };
    return {
        validateBuy: function(orderWin) {
            var message = "";
            orderWin_ = orderWin;
            var price = orderWin.spinPrice.get("value");
            var quantity = orderWin.spinQuantity.get("value");
            var minQuantity = orderWin.spinMinQuantity.get("value");
            var discloseQty = orderWin.spinDisclose.get("value");
            var securityId = orderWin.txtSecurity.get("value");
            var board = orderWin.cmbBoard.get("value");
            var qtyMultiplier = orderWin.qtyAndPriceMulArray[board][0];
            var priceMultiplier = orderWin.qtyAndPriceMulArray[board][1];
            var disclosePercentage = orderWin.diclosePercentArray[board];
            var account = orderWin.cmbClientAcc.get("value");
            var oldOrderValue = orderWin.oldOrderValue;
            var netOrderValue = orderWin.netOrderValue;
            var buyPower = orderWin.buyPower;
            var perDayRemLimit = orderWin.perDayLimitRemaining;
            var perOrdLimit = orderWin.perOrderLimit;
            var contraFirm = orderWin.contraFirm.get("value");
            var securityActive = orderWin.securityActive;
            var asset = orderWin.assetClass.get("value");
            var isCustodian = orderWin.isCustodian;
            var isSystemClient = orderWin.isSystemClient;
            var cusBuyPowerValidate = orderWin.cusBuyPowerValidate;
            var role = orderWin.role;
            var stopPx = orderWin.spinStopPrice.get("value");
            var isEnableMangerApproval = orderWin.isEnableMangerApproval;
            var marketData = eval(orderWin.marektDetails);
            var session = orderWin.orderStatus;
            var sessionId = orderWin.sessionId;
            var oddLotQty = orderWin.oddLotQty;
            var orderType = orderWin.cmbTif.get("value");
            var oldOrderQty = orderWin.oldOrderQty;
            var isPromoterClient = orderWin.isPromoterClient;
            var isPromoterSec = orderWin.isPromoterSec;
            var byPassPromoterSec = orderWin.byPassPromoterSec;
            var clientPANNumber = orderWin.clientPANNumber;
            var globalPANLimit = orderWin.globalPANLimit;
            var maxallowedqty = orderWin.maxallowedqty;
            var lowFieldVal = orderWin.lowfieldVal;
            var highFieldVal = orderWin.highfieldVal;
            var newAllowed = orderWin.newAllowed;
            var amendAllowed = orderWin.amendAllowed;
            var amendStatus = orderWin.amendStatus;
            var typeOfOrder = orderWin.cmbTypeOfOrder.get("value");
            if (account == null || account == "") {
                message = "javascriptOrderAccNotValid";
                return showError(message);
            }
            if (securityId == null || securityId == "") {
                message = "javascriptOrderSecNotValid";
                return showError(message);
            }
            if (quantity == null || quantity == "" || quantity <= 0) {
                message = "javascriptOrderQtyNotValid";
                return showError(message);
            }
            if (!(orderType == "20" || orderType == "28") && (price == null || price == "" || price <= 0)) {
                message = "javascriptOrderPriceNotValid";
                return showError(message);
            }
            if (securityActive == false) {
                message = "javascriptSecurityNotPermittedToTrade";
                return showError(message);
            }
            if (!amendStatus && !newAllowed) {
                message = "javascript3902";
                return showError(message);
            } else {
                if (amendStatus && !amendAllowed) {
                    message = "javascript3903";
                    return showError(message);
                }
            }
            if (typeOfOrder != null && typeOfOrder != 2 && quantity > maxallowedqty) {
                message = "javascriptSpnQtyExceedsMaxQty";
                return showError(message);
            }
            if ((typeOfOrder != null && typeOfOrder != 2) && (orderType != null && orderType != "20" && orderType != "28") && (price < lowFieldVal || price > highFieldVal)) {
                message = "javascriptSpnPriceNotValid";
                return showError(message, null, " " + lowFieldVal + " and " + highFieldVal);
            }
            if (discloseQty != null && discloseQty != "" && discloseQty > 0) {
                message = "javascriptOrderDiscloseQtyNotValid";
                return showError(message);
            }
            if (discloseQty > quantity) {
                message = "javascriptOrderDiscloseQtyExceedQty";
                return showError(message);
            }
            if (isPromoterSec == "Y" && byPassPromoterSec == "N" && isPromoterClient == "0") {
                message = "javascriptInvalidPromoters";
                return showError(message);
            }
            if (sessionId != null && sessionId == "111" && (+quantity <= +oddLotQty)) {
                message = "javascriptNoOddLotPreOpen";
                return showError(message);
            }
            if (sessionId != null && sessionId == "241" && (+quantity <= +oddLotQty)) {
                message = "javascriptNoOddLotSpePreOpen";
                return showError(message);
            }
            if (sessionId != null && sessionId == "241" && orderType != "16") {
                message = "javascriptSpePreOpenOrderTypeErr";
                return showError(message);
            }
            if (sessionId != null && sessionId == "111" && orderType != "16" && orderType != "20") {
                message = "javascriptPreOpenOrderTypeErr";
                return showError(message);
            }
            if (sessionId != null && sessionId == "121" && (+quantity <= +oddLotQty) && (orderType != "16" && orderType != "20")) {
                message = "javascriptNoOddLotDayMarket";
                return showError(message);
            }
            if (sessionId != null && sessionId == "121" && (+quantity <= +oddLotQty) && (+discloseQty != 0)) {
                message = "javascriptOddLotDiscloseQtyError";
                return showError(message);
            }
            if (orderType == "16" && (+quantity > +oddLotQty) && (+discloseQty != 0) && (+discloseQty < 10)) {
                message = "javascriptNonOddLotDiscloseQtyError";
                return showError(message);
            }
            if (orderType != "16" && (+discloseQty > 0)) {
                message = "javascriptOrderTypeDiscloseQtyError";
                return showError(message);
            }
            var minDisclose = quantity * (disclosePercentage / 100);
            minDisclose = Math.round(minDisclose);
            if (orderType == "16" && (+quantity > +oddLotQty) && (+discloseQty != 0) && (+discloseQty < +minDisclose)) {
                message = "javascriptNonOddLotDiscloseQtyMinError";
                return showError(message, null, minDisclose);
            }
            if (amendStatus == true && (+oldOrderQty <= +oddLotQty) && (+quantity > +oddLotQty)) {
                message = "javascriptOddLotAmendQtyError";
                return showError(message, null, oddLotQty);
            }
            if (amendStatus == true && (+oldOrderQty > +oddLotQty) && (+quantity <= +oddLotQty)) {
                message = "javascriptNonOddLotAmendQtyError";
                return showError(message, null, oddLotQty);
            }
            if (orderType == "528") {
                message = "javascriptNoAONBuyOrders";
                return showError(message);
            }
            if (quantity % qtyMultiplier > 0) {
                message = "javascriptOrderQtyMultplyNotValid";
                return showError(message, role + "_", qtyMultiplier);
            }
            var boardLength = marketData.market[0].assets.length;
            assetCode = 0;
            if (typeOfOrder != null && typeOfOrder != undefined && typeOfOrder != "2") {
                for (var i = 0; i < marketData.market[0].assets.length; i++) {
                    if (marketData.market[0].assets[i].code == asset) {
                        for (var j = 0; j < marketData.market[0].assets[i].boards.length; j++) {
                            if (marketData.market[0].assets[i].boards[j].value == board) {
                                for (var k = 0; k < marketData.market[0].assets[i].boards[j].pricemultiplies.length; k++) {
                                    var startval = eval(marketData.market[0].assets[i].boards[j].pricemultiplies[k].startValue);
                                    var endval = marketData.market[0].assets[i].boards[j].pricemultiplies[k].endValue;
                                    var multipy = eval(marketData.market[0].assets[i].boards[j].pricemultiplies[k].multiplier);
                                    if (price >= startval && price <= eval(endval)) {
                                        if (!Number.isInteger(Math.fround(price / multipy))) {
                                            message = "javascriptOrderPriceMultplyNotValid";
                                            return showError(message, role + "_", multipy);
                                        }
                                    } else {
                                        if (price >= startval && endval == "") {
                                            if (!Number.isInteger(Math.fround(price / multipy))) {
                                                message = "javascriptOrderPriceMultplyNotValid";
                                                return showError(message, role + "_", multipy);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (minQuantity > quantity) {
                message = "javascriptMinQtyNotValid";
                return showError(message);
            }
            if (board == 4 && contraFirm == "") {
                message = "javascriptContrafirmNotValid";
                return showError(message, role + "_");
            }
            var orderValue = netOrderValue - oldOrderValue;
            var orderValue_ = (+price * +quantity);
            if (asset == "1") {
                if (board == 4 && +orderValue_ < 20000000) {
                    message = "javascriptCrossingOrderValue";
                    return showError(message);
                }
            } else {
                if (board == 4 && +orderValue_ < 10000000) {
                    message = "javascriptCrossingOrderValueDebt";
                    return showError(message);
                }
            }
            if (board == 7 && orderValue > buyPower) {
                message = "javascriptOrderBuyPowerExceeds";
                var buypower = ATRAD.Util.addCommas(buyPower.toFixed(2));
                orderWin.approvalReasons = language.getString(message) + " " + buypower;
                orderWin.sendToApproval = "yes";
                return showError(message, "Manager", buypower, 1);
            }
            if ((isCustodian == "false" || (isCustodian == "true" && cusBuyPowerValidate == 1)) && isSystemClient == "true" && +orderValue > +buyPower) {
                message = "javascriptOrderBuyPowerExceeds";
                var buypower = ATRAD.Util.addCommas(buyPower.toFixed(2));
                orderWin.approvalReasons = language.getString(message) + " " + buypower;
                orderWin.sendToApproval = "yes";
                return showError(message, role, buypower, isEnableMangerApproval);
            }
            if (clientPANNumber != "-" && +orderValue_ > +perOrdLimit) {
                message = "javascriptOrderOrdLimitExceeds";
                orderWin.approvalReasons = language.getString(message) + " " + perOrdLimit;
                orderWin.sendToApproval = "yes";
                return showError(message, role, perOrdLimit, isEnableMangerApproval);
            }
            if (clientPANNumber == "-") {
                if (+globalPANLimit < +perOrdLimit && +orderValue_ > +globalPANLimit) {
                    message = "javascriptPANOrderOrdLimitExceeds";
                    return showError(message, null, globalPANLimit);
                } else {
                    if (+globalPANLimit > +perOrdLimit && +orderValue_ > +perOrdLimit) {
                        message = "javascriptOrderOrdLimitExceeds";
                        return showError(message, null, perOrdLimit);
                    }
                }
            }
            if (isSystemClient == "true" && +orderValue > +perDayRemLimit) {
                message = "javascriptOrderPerDayLimitExceeds";
                orderWin.approvalReasons = language.getString(message) + " " + perDayRemLimit;
                orderWin.sendToApproval = "yes";
                return showError(message, role, perDayRemLimit, isEnableMangerApproval);
            }
            if (isSystemClient == "false" && board == 7 && role != "Manager" && role != "SuperAdvisor") {
                message = "javascript3002";
                return showError(message, undefined, undefined, 0);
            }
            if (isSystemClient == "false" && board != 7 && role != "Manager") {
                message = "javascript3002";
                return showError(message, undefined, undefined, 0);
            }
            return true;
        },
        validateSell: function(orderWin) {
            var message = "";
            orderWin_ = orderWin;
            var price = orderWin.spinPrice.get("value");
            var quantity = orderWin.spinQuantity.get("value");
            var discloseQty = orderWin.spinDisclose.get("value");
            var minQuantity = orderWin.spinMinQuantity.get("value");
            var securityId = orderWin.txtSecurity.get("value");
            var board = orderWin.cmbBoard.get("value");
            var qtyMultiplier = orderWin.qtyAndPriceMulArray[board][0];
            var priceMultiplier = orderWin.qtyAndPriceMulArray[board][1];
            var disclosePercentage = orderWin.diclosePercentArray[board];
            var account = orderWin.cmbClientAcc.get("value");
            var asset = orderWin.assetClass.get("value");
            var isCustodian = orderWin.isCustodian;
            var securityActive = orderWin.securityActive;
            var amendedQuantity = orderWin.spinQuantity.get("value");
            var availableShares = orderWin.avalShares;
            var blotterQuantity = orderWin.orderQuantity;
            var amendStatus = orderWin.amendStatus;
            var isSystemClient = orderWin.isSystemClient;
            var role = orderWin.role;
            var contraFirm = orderWin.contraFirm.get("value");
            var stopPx = orderWin.spinStopPrice.get("value");
            var isEnableMangerApproval = orderWin.isEnableMangerApproval;
            var isShortSellEnable = orderWin.isShortSellEnable;
            var isDvpEnabled = orderWin.isDvpEnabled;
            var oddLotQty = orderWin.oddLotQty;
            var session = orderWin.orderStatus;
            var sessionId = orderWin.sessionId;
            var orderType = orderWin.cmbTif.get("value");
            var oldOrderQty = orderWin.oldOrderQty;
            var isPromoterClient = orderWin.isPromoterClient;
            var isPromoterSec = orderWin.isPromoterSec;
            var byPassPromoterSec = orderWin.byPassPromoterSec;
            var clientPANNumber = orderWin.clientPANNumber;
            var globalPANLimit = orderWin.globalPANLimit;
            var maxallowedqty = orderWin.maxallowedqty;
            var lowFieldVal = orderWin.lowfieldVal;
            var highFieldVal = orderWin.highfieldVal;
            var newAllowed = orderWin.newAllowed;
            var amendAllowed = orderWin.amendAllowed;
            var amendStatus = orderWin.amendStatus;
            var typeOfOrder = orderWin.cmbTypeOfOrder.get("value");
            if (account == null || account == "") {
                message = "javascriptOrderAccNotValid";
                return showError(message);
            }
            if (securityId == null || securityId == "") {
                message = "javascriptOrderSecNotValid";
                return showError(message);
            }
            if (quantity == null || quantity == "" || quantity <= 0) {
                message = "javascriptOrderQtyNotValid";
                return showError(message);
            }
            if (!(orderType == "20" || orderType == "28") && (price == null || price == "" || price <= 0)) {
                message = "javascriptOrderPriceNotValid";
                return showError(message);
            }
            if (securityActive == false) {
                message = "javascriptSecurityNotPermittedToTrade";
                return showError(message);
            }
            if (!amendStatus && !newAllowed) {
                message = "javascript3902";
                return showError(message);
            } else {
                if (amendStatus && !amendAllowed) {
                    message = "javascript3903";
                    return showError(message);
                }
            }
            if (typeOfOrder != null && typeOfOrder != 2 && quantity > maxallowedqty) {
                message = "javascriptSpnQtyExceedsMaxQty";
                return showError(message);
            }
            if ((typeOfOrder != null && typeOfOrder != 2) && (orderType != null && orderType != "20" && orderType != "28") && (price < lowFieldVal || price > highFieldVal)) {
                message = "javascriptSpnPriceNotValid";
                return showError(message, null, " " + lowFieldVal + " and " + highFieldVal);
            }
            if (discloseQty != null && discloseQty != "" && discloseQty > 0) {
                message = "javascriptOrderDiscloseQtyNotValid";
                return showError(message);
            }
            if (discloseQty > quantity) {
                message = "javascriptOrderDiscloseQtyExceedQty";
                return showError(message);
            }
            if (isPromoterSec == "Y" && byPassPromoterSec == "N" && isPromoterClient == "0") {
                message = "javascriptInvalidPromoters";
                return showError(message);
            }
            if (sessionId != null && sessionId == "111" && (+quantity <= +oddLotQty)) {
                message = "javascriptNoOddLotPreOpen";
                return showError(message);
            }
            if (sessionId != null && sessionId == "241" && (+quantity <= +oddLotQty)) {
                message = "javascriptNoOddLotSpePreOpen";
                return showError(message);
            }
            if (sessionId != null && sessionId == "241" && orderType != "16") {
                message = "javascriptSpePreOpenOrderTypeErr";
                return showError(message);
            }
            if (sessionId != null && sessionId == "111" && orderType != "16" && orderType != "20") {
                message = "javascriptPreOpenOrderTypeErr";
                return showError(message);
            }
            if (sessionId != null && sessionId == "121" && (+quantity <= +oddLotQty) && (orderType != "16" && orderType != "20")) {
                message = "javascriptNoOddLotDayMarket";
                return showError(message);
            }
            if (sessionId != null && sessionId == "121" && (+quantity <= +oddLotQty) && (+discloseQty != 0)) {
                message = "javascriptOddLotDiscloseQtyError";
                return showError(message);
            }
            if (amendStatus == true && (+oldOrderQty <= +oddLotQty) && (+quantity > +oddLotQty)) {
                message = "javascriptOddLotAmendQtyError";
                return showError(message, null, oddLotQty);
            }
            if (amendStatus == true && (+oldOrderQty > +oddLotQty) && (+quantity <= +oddLotQty)) {
                message = "javascriptNonOddLotAmendQtyError";
                return showError(message, null, oddLotQty);
            }
            if ((+availableShares > +oddLotQty) && (+quantity <= +oddLotQty)) {
                message = "javascriptOrderAvlQtyExceedOddLotQty";
                return showError(message, null, oddLotQty);
            }
            if (orderWin.bookDefId == "5" && !(orderWin.amendAllowed || orderWin.newAllowed)) {
                message = "javascriptAONQueuedOrderNotValid";
                return showError(message);
            }
            if (orderType == "16" && (+quantity > +oddLotQty) && (+discloseQty != 0) && (+discloseQty < 10)) {
                message = "javascriptNonOddLotDiscloseQtyError";
                return showError(message);
            }
            if (orderType != "16" && (+discloseQty > 0)) {
                message = "javascriptOrderTypeDiscloseQtyError";
                return showError(message);
            }
            var minDisclose = quantity * (disclosePercentage / 100);
            minDisclose = Math.round(minDisclose);
            if (orderType == "16" && (+quantity > +oddLotQty) && (+discloseQty != 0) && (+discloseQty < +minDisclose)) {
                message = "javascriptNonOddLotDiscloseQtyMinError";
                return showError(message, null, minDisclose);
            }
            if (quantity % qtyMultiplier > 0) {
                message = "javascriptOrderQtyMultplyNotValid";
                return showError(message, role + "_", qtyMultiplier);
            }
            if (board == 4 && contraFirm == "") {
                message = "javascriptContrafirmNotValid";
                return showError(message, role + "_");
            }
            var orderValue_ = (+price * +quantity);
            if (clientPANNumber == "-" && +orderValue_ > +globalPANLimit) {
                message = "javascriptPANOrderOrdLimitExceeds";
                return showError(message, null, globalPANLimit);
            }
            if (asset == "1") {
                if (board == 4 && orderValue_ < 20000000) {
                    message = "javascriptCrossingOrderValue";
                    return showError(message);
                }
            } else {
                if (board == 4 && orderValue_ < 10000000) {
                    message = "javascriptCrossingOrderValueDebt";
                    return showError(message);
                }
            }
            var marketData = eval(orderWin.marektDetails);
            if (typeOfOrder != null && typeOfOrder != undefined && typeOfOrder != "2") {
                for (var i = 0; i < marketData.market[0].assets.length; i++) {
                    if (marketData.market[0].assets[i].code == asset) {
                        for (var j = 0; j < marketData.market[0].assets[i].boards.length; j++) {
                            if (marketData.market[0].assets[i].boards[j].value == board) {
                                for (var k = 0; k < marketData.market[0].assets[i].boards[j].pricemultiplies.length; k++) {
                                    var startval = eval(marketData.market[0].assets[i].boards[j].pricemultiplies[k].startValue);
                                    var endval = marketData.market[0].assets[i].boards[j].pricemultiplies[k].endValue;
                                    var multipy = eval(marketData.market[0].assets[i].boards[j].pricemultiplies[k].multiplier);
                                    if (price >= startval && price <= eval(endval)) {
                                        if (!Number.isInteger(Math.fround(price / multipy))) {
                                            message = "javascriptOrderPriceMultplyNotValid";
                                            return showError(message, role + "_", multipy);
                                        }
                                    } else {
                                        if (price >= startval && endval == "") {
                                            if (!Number.isInteger(Math.fround(price / multipy))) {
                                                message = "javascriptOrderPriceMultplyNotValid";
                                                return showError(message, role + "_", multipy);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (minQuantity > quantity) {
                message = "javascriptMinQtyNotValid";
                return showError(message);
            }
            if (board == 7 && isSystemClient == "false" && (role == "Manager" || role == "SuperAdvisor")) {
                message = "javascriptOrdeCDSNotInSystem";
                orderWin.approvalReasons = language.getString(message);
                orderWin.sendToApproval = "yes";
                return showError(message, "Manager", undefined, 1);
            }
            if (board == 7 && isSystemClient == "false" && role != "Manager" && role != "SuperAdvisor") {
                message = "javascript3002";
                return showError(message, undefined, undefined, 0);
            }
            if (board == 7 && quantity > eval(availableShares) && isCustodian == "false") {
                message = "javascriptOrderAvlSharesExceeds";
                return showError(message);
            }
            if (board == 7 && isCustodian == "true") {
                message = "javascriptOrderAvlSharesExceedsShortCus";
                orderWin.approvalReasons = language.getString(message);
                orderWin.sendToApproval = "yes";
                return showError(message, "Manager", undefined, 1);
            }
            if (amendStatus == true) {
                if (isDvpEnabled == "true" && (isShortSellEnable == "true" || isCustodian == "true" || isSystemClient == "false") && !(eval(amendedQuantity) <= eval(blotterQuantity) + eval(availableShares))) {
                    message = "javascriptOrderAvlSharesExceedsShortSell";
                    if (isSystemClient == "false") {
                        message = "javascriptOrdeCDSNotInSystem";
                    }
                    orderWin.approvalReasons = language.getString(message);
                    orderWin.sendToApproval = "yes";
                    return showError(message, "Manager", undefined, 1);
                }
                if (isDvpEnabled == "true" && isShortSellEnable == "false" && !(eval(amendedQuantity) <= eval(blotterQuantity) + eval(availableShares))) {
                    message = "javascriptOrderAvlSharesExceeds";
                    orderWin.approvalReasons = language.getString(message);
                    orderWin.sendToApproval = "yes";
                    if (role == "SuperAdvisor" || role == "Advisor") {
                        return showError(message, role, availableShares, isEnableMangerApproval);
                    }
                    return showError(message, "", availableShares, 1);
                }
                if (isSystemClient == "true" && isCustodian == "false" && !(eval(amendedQuantity) <= eval(blotterQuantity) + eval(availableShares))) {
                    message = "javascriptOrderAvlSharesExceeds";
                    return showError(message, role, (eval(blotterQuantity) + eval(availableShares)), isEnableMangerApproval);
                }
                if ((eval(blotterQuantity) + eval(availableShares) <= +oddLotQty) && (+amendedQuantity > +oddLotQty)) {
                    message = "javascriptOrderAvlQtyBelowOddLotQty";
                    return showError(message, null, oddLotQty);
                }
            } else {
                if (isDvpEnabled == "true" && (isShortSellEnable == "true" || isCustodian == "true" || isSystemClient == "false") && quantity > eval(availableShares)) {
                    message = "javascript3002";
                    if (isSystemClient == "false" && role != "Manager") {
                        return showError(message, undefined, undefined, 0);
                    }
                    message = "javascriptOrderAvlSharesExceedsShortSell";
                    if (isSystemClient == "false") {
                        message = "javascriptOrdeCDSNotInSystem";
                    }
                    orderWin.approvalReasons = language.getString(message);
                    orderWin.sendToApproval = "yes";
                    return showError(message, "Manager", undefined, 1);
                }
                if (isDvpEnabled == "true" && isShortSellEnable == "false" && quantity > eval(availableShares)) {
                    message = "javascriptOrderAvlSharesExceeds";
                    orderWin.approvalReasons = language.getString(message) + " " + availableShares;
                    orderWin.sendToApproval = "yes";
                    if (role == "SuperAdvisor" || role == "Advisor") {
                        return showError(message, role, availableShares, isEnableMangerApproval);
                    }
                    return showError(message, "", availableShares, 1);
                }
                if (isSystemClient == "true" && isCustodian == "false" && quantity > eval(availableShares)) {
                    message = "javascriptOrderAvlSharesExceeds";
                    orderWin.approvalReasons = language.getString(message) + " " + availableShares;
                    orderWin.sendToApproval = "yes";
                    return showError(message, role, availableShares, isEnableMangerApproval);
                }
                if (isSystemClient == "false" && role != "Manager") {
                    message = "javascript3002";
                    return showError(message, undefined, undefined, 0);
                }
                if ((+availableShares > +0) && (+availableShares <= +oddLotQty) && (+quantity > +oddLotQty)) {
                    message = "javascriptOrderAvlQtyBelowOddLotQty";
                    return showError(message, null, oddLotQty);
                }
            }
            return true;
        },
        validateAmend: function(orderWin) {
            var message = "";
        },
        validateBasketBuy: function(orderWin) {
            var message = "";
            orderWin_ = orderWin;
            var price = orderWin.spinPrice.get("value");
            var quantity = orderWin.spinQuantity.get("value");
            var securityId = orderWin.txtSecurity.get("value");
            var board = "1";
            var qtyMultiplier = orderWin.qtyAndPriceMulArray[board][0];
            var account = orderWin.cmbClientAcc.get("value");
            var oldOrderValue = orderWin.oldOrderValue;
            var netOrderValue = orderWin.netOrderValue;
            var securityActive = orderWin.securityActive;
            var asset = orderWin.assetClass.get("value");
            var orderType = orderWin.cmbTif.get("value");
            var stopPx = orderWin.spinStopPrice.get("value");
            var typeOfOrder = orderWin.cmbTypeOfOrder.get("value");
            var newAllowed = orderWin.newAllowed;
            var amendAllowed = orderWin.amendAllowed;
            var amendStatus = orderWin.amendStatus;
            var maxallowedqty = orderWin.maxallowedqty;
            var lowFieldVal = orderWin.lowfieldVal;
            var highFieldVal = orderWin.highfieldVal;
            var isPromoterClient = orderWin.isPromoterClient;
            var isPromoterSec = orderWin.isPromoterSec;
            var byPassPromoterSec = orderWin.byPassPromoterSec;
            var sessionId = orderWin.sessionId;
            var oddLotQty = orderWin.oddLotQty;
            var role = "basket";
            if (!amendStatus && !newAllowed) {
                message = "javascript39021";
                return showError(message);
            } else {
                if (amendStatus && !amendAllowed) {
                    message = "javascript39031";
                    return showError(message);
                }
            }
            if (!(orderType == "16" || orderType == "32" || orderType == "1")) {
                message = "javascriptBasketOrderTypeError";
                return showError(message);
            }
            if (asset != "1") {
                message = "javascriptBasketOrderAssetError";
                return showError(message);
            }
            if (account == null || account == "") {
                message = "javascriptOrderAccNotValid";
                return showError(message);
            }
            if (securityId == null || securityId == "") {
                message = "javascriptOrderSecNotValid";
                return showError(message);
            }
            if (quantity == null || quantity == "" || quantity <= 0) {
                message = "javascriptOrderQtyNotValid";
                return showError(message);
            }
            if (!(orderType == "20" || orderType == "28") && (price == null || price == "" || price <= 0)) {
                message = "javascriptOrderPriceNotValid";
                return showError(message);
            }
            if (securityActive == false) {
                message = "javascriptSecurityNotPermittedToTrade";
                return showError(message);
            }
            if (quantity % qtyMultiplier > 0) {
                message = "javascriptOrderQtyMultplyNotValid";
                return showError(message, role, qtyMultiplier);
            }
            if ((orderType == "24" || orderType == "28") && (stopPx == null || stopPx <= 0)) {
                message = "javascriptOrderStopPriceNotValid";
                return showError(message);
            }
            if (typeOfOrder != null && !(typeOfOrder == 1 || typeOfOrder == 7)) {
                message = "javascriptBasketOrderTypeOfOrderError";
                return showError(message);
            }
            if (isPromoterSec == "Y" && byPassPromoterSec == "N" && isPromoterClient == "0") {
                message = "javascriptInvalidPromoters";
                return showError(message);
            }
            if (sessionId != null && sessionId == "111" && (+quantity <= +oddLotQty)) {
                message = "javascriptNoOddLotPreOpen";
                return showError(message);
            }
            if (sessionId != null && sessionId == "241" && (+quantity <= +oddLotQty)) {
                message = "javascriptNoOddLotSpePreOpen";
                return showError(message);
            }
            if (sessionId != null && sessionId == "241" && orderType != "16") {
                message = "javascriptSpePreOpenOrderTypeErr";
                return showError(message);
            }
            if (sessionId != null && sessionId == "111" && orderType != "16" && orderType != "20") {
                message = "javascriptPreOpenOrderTypeErr";
                return showError(message);
            }
            if (sessionId != null && sessionId == "121" && (+quantity <= +oddLotQty) && (orderType != "16" && orderType != "20")) {
                message = "javascriptNoOddLotDayMarket";
                return showError(message);
            }
            var marketData = eval(orderWin.marektDetails);
            for (var i = 0; i < marketData.market[0].assets.length; i++) {
                if (marketData.market[0].assets[i].code == asset) {
                    for (var j = 0; j < marketData.market[0].assets[i].boards.length; j++) {
                        if (marketData.market[0].assets[i].boards[j].value == board) {
                            for (var k = 0; k < marketData.market[0].assets[i].boards[j].pricemultiplies.length; k++) {
                                var startval = eval(marketData.market[0].assets[i].boards[j].pricemultiplies[k].startValue);
                                var endval = marketData.market[0].assets[i].boards[j].pricemultiplies[k].endValue;
                                var multipy = eval(marketData.market[0].assets[i].boards[j].pricemultiplies[k].multiplier);
                                if (price >= startval && price <= eval(endval)) {
                                    if (!Number.isInteger(Math.fround(price / multipy))) {
                                        message = "javascriptOrderPriceMultplyNotValid";
                                        return showError(message, role + "_", multipy);
                                    }
                                } else {
                                    if (price >= startval && endval == "") {
                                        if (!Number.isInteger(Math.fround(price / multipy))) {
                                            message = "javascriptOrderPriceMultplyNotValid";
                                            return showError(message, role + "_", multipy);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return true;
        },
        validateBasketSell: function(orderWin) {
            var message = "";
            orderWin_ = orderWin;
            var price = orderWin.spinPrice.get("value");
            var quantity = orderWin.spinQuantity.get("value");
            var securityId = orderWin.txtSecurity.get("value");
            var board = "1";
            var qtyMultiplier = orderWin.qtyAndPriceMulArray[board][0];
            var account = orderWin.cmbClientAcc.get("value");
            var asset = orderWin.assetClass.get("value");
            var securityActive = orderWin.securityActive;
            var role = orderWin.role;
            var orderType = orderWin.cmbTif.get("value");
            var typeOfOrder = orderWin.cmbTypeOfOrder.get("value");
            var stopPx = orderWin.spinStopPrice.get("value");
            var typeOfOrder = orderWin.cmbTypeOfOrder.get("value");
            var newAllowed = orderWin.newAllowed;
            var amendAllowed = orderWin.amendAllowed;
            var amendStatus = orderWin.amendStatus;
            var maxallowedqty = orderWin.maxallowedqty;
            var lowFieldVal = orderWin.lowfieldVal;
            var highFieldVal = orderWin.highfieldVal;
            var isPromoterClient = orderWin.isPromoterClient;
            var isPromoterSec = orderWin.isPromoterSec;
            var byPassPromoterSec = orderWin.byPassPromoterSec;
            var sessionId = orderWin.sessionId;
            var oddLotQty = orderWin.oddLotQty;
            var role = "basket";
            if (!amendStatus && !newAllowed) {
                message = "javascript39021";
                return showError(message);
            } else {
                if (amendStatus && !amendAllowed) {
                    message = "javascript39031";
                    return showError(message);
                }
            }
            if (!(orderType == "16" || orderType == "32" || orderType == "1")) {
                message = "javascriptBasketOrderTypeError";
                return showError(message);
            }
            if (asset != "1") {
                message = "javascriptBasketOrderAssetError";
                return showError(message);
            }
            if (account == null || account == "") {
                message = "javascriptOrderAccNotValid";
                return showError(message);
            }
            if (securityId == null || securityId == "") {
                message = "javascriptOrderSecNotValid";
                return showError(message, role);
            }
            if (quantity == null || quantity == "" || quantity <= 0) {
                message = "javascriptOrderQtyNotValid";
                return showError(message);
            }
            if (!(orderType == "20" || orderType == "28") && (price == null || price == "" || price <= 0)) {
                message = "javascriptOrderPriceNotValid";
                return showError(message);
            }
            if (securityActive == false) {
                message = "javascriptSecurityNotPermittedToTrade";
                return showError(message);
            }
            if (quantity % qtyMultiplier > 0) {
                message = "javascriptOrderQtyMultplyNotValid";
                return showError(message, role, qtyMultiplier);
            }
            if ((orderType == "24" || orderType == "28") && (stopPx == null || stopPx <= 0)) {
                message = "javascriptOrderStopPriceNotValid";
                return showError(message);
            }
            if (typeOfOrder != null && !(typeOfOrder == 1 || typeOfOrder == 7)) {
                message = "javascriptBasketOrderTypeOfOrderError";
                return showError(message);
            }
            if (typeOfOrder != null && typeOfOrder != 2 && quantity > maxallowedqty) {
                message = "javascriptSpnQtyExceedsMaxQty";
                return showError(message);
            }
            if ((typeOfOrder != null && typeOfOrder != 2) && (price < lowFieldVal || price > highFieldVal)) {
                message = "javascriptSpnPriceNotValid";
                return showError(message, null, " " + lowFieldVal + " and " + highFieldVal);
            }
            if (isPromoterSec == "Y" && byPassPromoterSec == "N" && isPromoterClient == "0") {
                message = "javascriptInvalidPromoters";
                return showError(message);
            }
            if (sessionId != null && sessionId == "111" && (+quantity <= +oddLotQty)) {
                message = "javascriptNoOddLotPreOpen";
                return showError(message);
            }
            if (sessionId != null && sessionId == "241" && (+quantity <= +oddLotQty)) {
                message = "javascriptNoOddLotSpePreOpen";
                return showError(message);
            }
            if (sessionId != null && sessionId == "241" && orderType != "16") {
                message = "javascriptSpePreOpenOrderTypeErr";
                return showError(message);
            }
            if (sessionId != null && sessionId == "111" && orderType != "16" && orderType != "20") {
                message = "javascriptPreOpenOrderTypeErr";
                return showError(message);
            }
            if (sessionId != null && sessionId == "121" && (+quantity <= +oddLotQty) && (orderType != "16" && orderType != "20")) {
                message = "javascriptNoOddLotDayMarket";
                return showError(message);
            }
            var marketData = eval(orderWin.marektDetails);
            for (var i = 0; i < marketData.market[0].assets.length; i++) {
                if (marketData.market[0].assets[i].code == asset) {
                    for (var j = 0; j < marketData.market[0].assets[i].boards.length; j++) {
                        if (marketData.market[0].assets[i].boards[j].value == board) {
                            for (var k = 0; k < marketData.market[0].assets[i].boards[j].pricemultiplies.length; k++) {
                                var startval = eval(marketData.market[0].assets[i].boards[j].pricemultiplies[k].startValue);
                                var endval = marketData.market[0].assets[i].boards[j].pricemultiplies[k].endValue;
                                var multipy = eval(marketData.market[0].assets[i].boards[j].pricemultiplies[k].multiplier);
                                if (price >= startval && price <= eval(endval)) {
                                    if (!Number.isInteger(Math.fround(price / multipy))) {
                                        message = "javascriptOrderPriceMultplyNotValid";
                                        return showError(message, role + "_", multipy);
                                    }
                                } else {
                                    if (price >= startval && endval == "") {
                                        if (!Number.isInteger(Math.fround(price / multipy))) {
                                            message = "javascriptOrderPriceMultplyNotValid";
                                            return showError(message, role + "_", multipy);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return true;
        }
    };
}
)();
