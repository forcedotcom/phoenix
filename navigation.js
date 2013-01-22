/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
function highlightTab() {
    var el = document.getElementById("content");
    var c = el.firstElementChild;
    var name = c.getAttribute("id") + "Tab";
    document.getElementById(name).setAttribute("class", "activeTab");
}
