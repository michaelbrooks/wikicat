(function () {

    var ns = walker;

    var populateTableBody = function(tableWrapper, rows) {
        var template = _.template(tableWrapper.find('.row-template').text());
        var tbody = tableWrapper.find('tbody');
        tbody.empty()

        for (var i in rows) {
            tbody.append(template(rows[i]));
        }

        tableWrapper.find('.count').text(rows.length);
    };

    var listResults = function(resultsElement, results) {
        var confirmMsg = resultsElement.find('.confirm-message');
        var searchConfirm = confirmMsg.find('.search-string');

        var categoriesList = resultsElement.find('.categories');
        var articlesList = resultsElement.find('.articles');

        if (results) {
            searchConfirm.text(results.search);
            confirmMsg.addClass('in');

            populateTableBody(
                categoriesList,
                results.categories
            );
            categoriesList.addClass('in');

            populateTableBody(
                articlesList,
                results.articles
            );
            articlesList.addClass('in');
        } else {
            confirmMsg.removeClass('in');
            categoriesList.removeClass('in');
            articlesList.removeClass('in');
        }
    };

    var submitQuery = function(searchForm, resultsElement, query) {
        var formData = {
            search: query
        };

        var loadingMsg = resultsElement.find('.loading-message');
        var confirmMsg = resultsElement.find('.confirm-message');
        confirmMsg.removeClass('in');
        loadingMsg.addClass('in');

        $.post('', formData)
            .done(function(results) {
                listResults(resultsElement, results);
            })
            .always(function() {
                loadingMsg.removeClass('in');
            });
    };

    $(document).ready(function () {
        var searchForm = $('.search-form');
        var searchField = searchForm.find('input[name=search]')

        var resultsElement = $('.results');
        var lastSearch = "";

        searchForm.submit(function(e) {
            e.preventDefault();

            var query = searchField.val();
            if (query !== lastSearch) {

                if (query) {
                    submitQuery(searchForm, resultsElement, query)
                } else {
                    listResults(resultsElement);
                }

                lastSearch = query;

                if (window.history && window.history.pushState) {
                    var title = "WikiWalker";
                    var url = "/walker";
                    if (query) {
                        title = "WikiWalker: '" + query + "'";
                        url = "/walker?q=" + encodeURIComponent(query);
                    }
                    window.history.pushState(query, title, url);
                }
            }

            return false;
        });

        //there could be an initial search we want to show
        if (searchForm.data('search')) {
            lastSearch = searchForm.data('search');
            submitQuery(searchForm, resultsElement, lastSearch);
        }

        ns.idToggleButton();
    });

})();