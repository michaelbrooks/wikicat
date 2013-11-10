(function() {

    var appendEntityLink = function(selection, property) {
        property = property || 'name';

        links = selection.append('a')
            .attr('href', function(d) {
                return d.url
            });

        links
            .text(function(d) {
                return d[property] + " "
            })
            .append('span')
            .classed('id hidden', true)
            .text(function(d) {
                return d.id;
            });

        return links;
    };

    var makeSuperCategories = function(supercatBox, data) {
        supercategories = supercatBox.selectAll('div.supercategory')
            .data(data)
            .enter()
            .append('div')
            .classed('supercategory', true);
        appendEntityLink(supercategories, 'short_name');
    };

    var makeSubCategories = function(subcatBox, data) {
        subcategories = subcatBox.selectAll('div.supercategory')
            .data(data)
            .enter()
            .append('div')
            .classed('subcategory', true);
        appendEntityLink(subcategories, 'short_name');
    };

    var makeArticles = function(articleBox, data) {
        articles = articleBox.selectAll('div.articles')
            .data(data)
            .enter()
            .append('div')
            .classed('article', true);

        appendEntityLink(articles);
    };

    var buildGlyph = function(versionEl, data) {
        var glyph = versionEl.find('.glyph');

        var supercatBox = d3.select(glyph.find('.supercategories')[0]);
        var subcatBox = d3.select(glyph.find('.subcategories')[0]);
        var articleBox = d3.select(glyph.find('.articles')[0]);

        makeSuperCategories(supercatBox, data.supercategories);
        makeSubCategories(subcatBox, data.subcategories);
        makeArticles(articleBox, data.articles);

        var loading = versionEl.find('.loading');
        loading.removeClass('in').on($.support.transition.end, function() {
            loading.addClass('hidden');
        });
    };

    var idToggleButton = function() {
        $('#id-toggle-button').click(function() {
            $this = $(this);
            if ($this.is('.active')) {
                $('.id').addClass('hidden');
                $this.text('Show IDs');
            } else {
                $('.id').removeClass('hidden');
                $this.text('Hide IDs');
            }
        });
    };

    //Export this namespace
    window.walker = {
        idToggleButton: idToggleButton,
        buildGlyph: buildGlyph
    };
})();