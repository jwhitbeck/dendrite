// Minimize with:
// uglifyjs --compress --mangle -- res/site.js > static/js/site.min.js
var makeMenu = function() {
    var sidebar = document.getElementsByClassName("sidebar")[0],
        el = document.getElementById("menu"),
        Menu = {
            init: function (el) {
                this.el = el;
                this.sidebar = sidebar;
                this.addEventListeners();
            },
            addEventListeners: function () {
                var that = this;
                this.el.querySelector("a").addEventListener('click', function() {
                    that.toogleVisibility();
                }, false);
            },
            toogleVisibility: function() {
                if (this.el.classList.contains("clicked")) {
                    this.hide();
                } else {
                    this.show();
                }
            },
            hide: function() {
                this.el.classList.remove("clicked");
                this.sidebar.style.display = "none";
            },
            show: function() {
                this.el.classList.add("clicked");
                this.sidebar.style.display = "block";
            }
        },
        menu = Object.create(Menu);
    menu.init(el);
}
makeMenu();
