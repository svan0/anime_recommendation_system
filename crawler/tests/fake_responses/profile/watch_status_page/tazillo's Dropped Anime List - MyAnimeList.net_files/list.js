$(document).ready(function() {
	$(".List_LightBox").fancybox({
		'width'				: 980,
		'height'			: '85%',
        'autoScale'			: true,
        'autoDimensions'	: true,
        'transitionIn'		: 'none',
		'transitionOut'		: 'none',
		'type'				: 'iframe'
	});

	$('#searchBox').focus(function(){
		if ($(this).val() == 'Search') {
			$(this).val('');
		}

		$(this).select();
	});

	$('#searchBox').blur(function(){
		if ($(this).val() == '') {
			$(this).val('Search');
		}
	});

	$('#searchBox').keydown(function(e){
		if (e.keyCode == '13') {
			$("#searchListButton").trigger('performListSearch');
		}
	});

	$("#searchListButton").bind("performListSearch",function(){
		window.location = '/'+$('#listType').val()+'list/'+$('#listUserName').val()+'?s='+$('#searchBox').val();
	});

	$("#searchListButton").click(function(){
		$("#searchListButton").trigger('performListSearch');
	});
});

function changeep_loadurl(list_entry_id, totalEpisodes, animeStatus, uStatus, inepVal, animeid, clickedPlus)
{
	if (clickedPlus == null) {
		clickedPlus = false;
	}

	var episode_val = document.getElementById('epID'+list_entry_id);
	var myVal = episode_val.value;
	var epNum;

	if (inepVal == null || inepVal == 0)
		epNum = myVal;
	else
		epNum = inepVal;

	epNum = Number(epNum);

	if ( isNaN(epNum) || ( (epNum>totalEpisodes) && (totalEpisodes != 0) ) || (epNum<0) )
		{
		alert('Invalid number supplied, try again.');
		return false;
		}
	else {
		var data = {
			"anime_id"            : parseInt(animeid),
			"status"							: parseInt(uStatus),
			"num_watched_episodes": parseInt(epNum) || 0,
		};
		var doPost = function (update) {
			$.post("/ownlist/anime/edit.json", JSON.stringify(update), function (data) {
				document.getElementById("output" + list_entry_id).innerText = epNum;
				var dynamicEpNum = document.getElementById("holdDynamicEpNum" + list_entry_id);
				dynamicEpNum.innerText = epNum + 1;

				function lightboxProcedure() {
					askToDiscuss(epNum, animeid, list_entry_id);

					if (!clickedPlus) {
						determineEpVisibility(list_entry_id);
					}

					episode_val.value = '';
				}

				window.MAL.SNSFunc.postListUpdates(data, 'anime', parseInt(animeid), {
					onComplete: function () {
						setTimeout(lightboxProcedure, 400);
					}
				});
			});
		};

		if ((uStatus == 2) && (totalEpisodes == epNum)) {
			fancyConfirm("Do you want to set this show as finished rewatching?", "Yes, I am done Rewatching", "Not Finished Rewatching", function (result) {
				if (result) {
					doneRewatching(animeid).always(function () {
						doPost(data);
					});
				} else {
					doPost(data);
				}
			});
		}
		else if ((totalEpisodes == epNum) && (animeStatus == 2)) // ask to set to completed, anime series status has to be completed (finished airing
		{
			fancyConfirm("Do you want to set this entry as completed?", "Set as Completed", "Do not set as Completed", function (result) {
				if (result) {
					markComplete(list_entry_id, epNum, animeid).always(function () {
						// ユーザがCompletedにセットしたので、アップデートする情報もステータスを直してやる
						data.status = 2;
						doPost(data);
					});
				} else {
					doPost(data);
				}
			});
		}
		else if ((uStatus == 3) || (uStatus == 6)) // if userStatus is on-hold or plan to watch
		{
			fancyConfirm("Move entry to watching?", "Set as Watching", "Do not Move", function (result) {
				if (result) {
					markWatching(list_entry_id, epNum, animeid).always(function () {
						// ユーザがWatchingにセットしたので、アップデートする情報もステータスを直してやる
						data.status = 1;
						doPost(data);
					});
				} else {
					doPost(data);
				}
			});
		}
		else {
			doPost(data);
		}

		return false;
	}
}

function doneRewatching(anime_id)
{
	return $.post("/includes/ajax.inc.php?t=59", {aid:anime_id});
}

function doneRereading(manga_id)
{
	return $.post("/includes/ajax.inc.php?t=81", {mid:manga_id});
}

function notdoneRewatching()
{
	$.fancybox.close();
}

function askToDiscuss(ep,animeid,list_id)
{
	$.post("/includes/ajax.inc.php?t=50", {epNum:ep, aid:animeid, id:list_id}, function(data)
		{
		var myRegExp = /trueEp/;
		var matchPos = data.search(myRegExp);

		if (matchPos != -1) // found it, so display lightbox
			{
				$.fancybox({
					'content': '<div style="font-family: verdana, arial; font-size: 11px; text-align: center;">'+data+'</div>',
			        'autoScale'			: true,
			        'autoDimensions'	: true,
				});
			}
		}
	);
}

function notthisseries(list_id)
{
	$.post("/includes/ajax.inc.php?t=53", {id:list_id}, function(data){ $.fancybox.close(); });
}

function dontAsk()
{
	$.post("../includes/ajax.inc.php?t=51", {y:1}, function(data){ $.fancybox.close(); });
}

function doNotMove(epNum,animeid,list_id)
{
	askToDiscuss(epNum,animeid,list_id);
}

function markWatching(list_entry_id,epNum,animeid)
{
	return $.post("/includes/ajax.inc.php?t=21", {eid:list_entry_id});
}

function markComplete (list_entry_id, epNum, animeid) {
	return $.post("/includes/ajax.inc.php?t=18", {aid: animeid});
}

function updateEpNumByOne(list_entry_id, totalEpisodes, animeStatus, uStatus, animeid)
{
	var dynamicEpNum = document.getElementById("holdDynamicEpNum"+list_entry_id);
	var dynamicEpNumVal = Number(dynamicEpNum.innerHTML);

	changeep_loadurl(list_entry_id,totalEpisodes,animeStatus,uStatus,dynamicEpNumVal,animeid,true);
}

function determineEpVisibility(layer_id)
{

    var myLayer = document.getElementById('epLayer'+layer_id);
    var visibleVal = (myLayer.style.display != "none");
    var myText = document.getElementById('epText'+layer_id);


    if (visibleVal)
			{
			//key.innerHTML = "+";
            // hide
			myLayer.style.display = "none";
	 		myText.style.fontWeight = "normal";
			}
		else
			{
			//key.innerHTML = "-";
            //show

			myLayer.style.display = "block";
			myText.style.fontWeight = "bold";
            document.getElementById('epID'+layer_id).focus();
			}

}

function tag_add(aid,type)
{
	var url;
	var tagString = document.getElementById("tagInfo"+aid).value;
	document.getElementById("tagRowEdit"+aid).style.display = 'none';
	document.getElementById("tagLinks"+aid).style.display = 'block';
	document.getElementById("tagLinks"+aid).innerHTML = 'Saving...';
	document.getElementById("tagRow"+aid).innerText = tagString;

	if (type == 1) {
		url = "/includes/ajax.inc.php?t=22&tags=" + encodeURIComponent(tagString);
		$.post(url, {aid:aid}, function (data__safe) {
			//alert(data);
			document.getElementById("tagLinks" + aid).innerHTML = data__safe;
			document.getElementById("tagLinks" + aid).style.display = 'block';
			document.getElementById("tagChangeRow" + aid).style.display = 'block';
		}
		);
	}
	else {
		url = "/includes/ajax.inc.php?t=30&tags=" + encodeURIComponent(tagString);
		$.post(url, {mid:aid}, function (data__safe) {
			//alert(data);
			document.getElementById("tagLinks" + aid).innerHTML = data__safe;
			document.getElementById("tagLinks" + aid).style.display = 'block';
			document.getElementById("tagChangeRow" + aid).style.display = 'block';
		}
		);
	}
}

function tag_showEdit(aid,type)
{
	var oldTags = document.getElementById("tagRow"+aid).innerText;
	document.getElementById("tagLinks"+aid).style.display = 'none';
	document.getElementById("tagChangeRow"+aid).style.display = 'none';
	var tagRowEditObj = document.getElementById("tagRowEdit"+aid);
	tagRowEditObj.style.display = 'block';
	tagRowEditObj.innerHTML = _.template('<textarea id="tagInfo<%- aid %>" onkeydown="tag_checkEnter(event,<%- aid %>,<%- type %>);"  class="inputtext" cols="12" rows="3"><%- oldTags %></textarea><div style="margin-top: 3px;"><span style="float: right;"><input type="button" value="Save" onclick="tag_add(<%- aid %>,<%- type %>)"></span><span style="float: left;"><input type="button" value="Cancel" onclick="tag_cancelAdd(<%- aid %>)"></span></div>')({'aid': Number(aid),'type': Number(type),'oldTags': oldTags});
	document.getElementById("tagInfo"+aid).focus();
}

function tag_cancelAdd(aid)
{
	//alert('test');
	var oldTags = document.getElementById("tagRow"+aid).value;
	document.getElementById("tagLinks"+aid).style.display = 'block';
	document.getElementById("tagChangeRow"+aid).style.display = 'block';
	document.getElementById("tagRowEdit"+aid).style.display = 'none';
}

function tag_checkEnter(e,aid,type)
{
	var key;

	 if(window.event)
		  key = window.event.keyCode;     //IE
	 else
		  key = e.which;     //firefox

	 if (key == 13)
		{
		tag_add(aid,type);
		return false;
		}
	 else
		return true;
}

function manga_updateVol(id,type,manga_id,volumes,status)
{
	var volObj = document.getElementById("vol"+id);
	var curVol = volObj.innerHTML;
	var volNum = 0;

	if (type == 1) // update normally
		{
		volNum = document.getElementById("voltext"+id).value;
		}
	else // update through "+"
		{
		if (isNaN(curVol))
			curVol = 0;

		volNum = Number(curVol) + 1;
		}

	volNum = Number(volNum);

	if ( isNaN(volNum) || (volNum<0) )
		{
		alert('Invalid number supplied, try again.');
		return false;
		}
	else {
		document.getElementById("voldiv" + id).style.display = 'none';

		$.post("/ownlist/manga/edit.json", JSON.stringify({
			"manga_id": parseInt(manga_id),
			"status": parseInt(status),
			"num_read_volumes": parseInt(volNum) || 0,
		}), function (data) {
			volObj.innerText = volNum;
			document.getElementById("voltext" + id).value = '';

			window.MAL.SNSFunc.postListUpdates(data, 'manga', parseInt(manga_id), {});
		});
	}
}

function manga_updateChap(id,type,manga_id,total_chapters,curStatus)
{
	var chapObj = document.getElementById("chap"+id);
	var curChap = chapObj.innerHTML;
	var chapNum = 0;

	if (type == 1) // update normally
		{
		chapNum = document.getElementById("chaptext"+id).value;
		}
	else // update through "+"
		{
		if (isNaN(curChap))
			curChap = 0;

		chapNum = Number(curChap) + 1;
		}

	chapNum = Number(chapNum);


	//alert(chapNum);
	if ( isNaN(chapNum) || (chapNum<0) ) {
		alert('Invalid number supplied, try again.');
		return false;
	} else {
		document.getElementById("chapdiv"+id).style.display = 'none';
		var data = {
			"manga_id"         : parseInt(manga_id),
			"status"			     : parseInt(curStatus),
			"num_read_chapters": parseInt(chapNum) || 0,
		};
		var doPost = function (update) {
			$.post("/ownlist/manga/edit.json", JSON.stringify(update), function (data) {
				chapObj.innerText = chapNum;
				document.getElementById("chaptext" + id).value = '';

				function lightboxProcedure() {
					manga_askToDiscuss(chapNum, manga_id, id);
				}

				window.MAL.SNSFunc.postListUpdates(data, 'manga', parseInt(manga_id), {
					onComplete: function () {
						setTimeout(lightboxProcedure, 400);
					}
				});
			});
		};

		if ((curStatus == 2) && (total_chapters == chapNum)) {
			fancyConfirm("Set this series as finished rereading?", "Yes, I am done Rereading", "Not Finished", function (result) {
				if (result) {
					doneRereading(manga_id).always(function () {
						doPost(data);
					});
				} else {
					doPost(data);
				}
			});
		}
		else if (total_chapters == chapNum) // ask to set as completed
		{
			fancyConfirm("Set this entry as completed?", "Set as Completed", "Do not set as Completed", function (result) {
				if (result) {
					manga_markComplete(id, chapNum, manga_id).always(function () {
						// ユーザがCompletedにセットしたので、アップデートする情報もステータスを直してやる
						data.status = 2;
						doPost(data);
					});
				} else {
					doPost(data);
				}
			});
		}
		else if ((curStatus == 3) || (curStatus == 6)) // if userStatus is on-hold or plan to watch
		{
			fancyConfirm("Move entry to reading?", "Set as Reading", "Do not Move", function (result) {
				if (result) {
					manga_markReading(id, chapNum, manga_id).always(function () {
						// ユーザがReadingにセットしたので、アップデートする情報もステータスを直してやる
						data.status = 1;
						doPost(data);
					});
				} else {
					doPost(data);
				}
			});
		} else {
			doPost(data);
		}
	}
}

function manga_markReading(list_entry_id,chap,manga_id)
{
	return $.post("/includes/ajax.inc.php?t=57", {id:list_entry_id});
}

function manga_askToDiscuss(chapter,manga_id,list_id)
{
	$.post("/includes/ajax.inc.php?t=55", {chap:chapter,mid:manga_id,id:list_id}, function(data){
		var myRegExp = /trueChap/;
		var matchPos = data.search(myRegExp);
		//alert('test');
		if (matchPos != -1) // found it, so display TB
		{
			$.fancybox({
				'content': '<div style="font-family: verdana, arial; font-size: 11px; text-align: center;">'+data+'</div>',
				'autoScale'			: true,
				'autoDimensions'	: true
			});
		}
	});
}

function manga_notthisseries(list_id)
{
	$.post("/includes/ajax.inc.php?t=56", {id:list_id}, function(data) { $.fancybox.close(); });
}

function manga_dontAsk()
{
	$.post("/includes/ajax.inc.php?t=71", {y:1}, function(data) { $.fancybox.close(); });
}

function manga_markComplete(entry_id,chap,manga_id)
{
	return $.post("/includes/ajax.inc.php?t=54", {mid:manga_id});
}

function manga_doNotMove(chap,manga_id,id)
{
	$.fancybox.close();
	manga_askToDiscuss(chap,manga_id,id);
}


function manga_showTextVol(id)
{
	var obj = document.getElementById("voldiv"+id);
	$(obj).toggle();
	document.getElementById("voltext"+id).focus();
}

function manga_showTextChap(id)
{
	var obj = document.getElementById("chapdiv"+id);
	$(obj).toggle();
	document.getElementById("chaptext"+id).focus();
}

function manga_checkEnter(e,type,id,manga_id,total_chapters,user_status)
{
	var key;

     if(window.event)
          key = window.event.keyCode;     //IE
     else
          key = e.which;     //firefox

	if (key == 13)
	 	{
	 	if (type == 1) // chapters
			manga_updateChap(id,1,manga_id,total_chapters,user_status);
		else if (type == 2) // volumes
			manga_updateVol(id,1,manga_id,total_chapters,user_status);
		else if (type == 3)
			manga_updateScore(id,manga_id);
		}
     else
          return true;
}

function manga_toggleScore(id)
{
	$('#scorediv'+id).toggle();
	document.getElementById("scoretext"+id).focus();
}

function manga_updateScore(id,manga_id)
{
	var newScore = document.getElementById("scoretext"+id).value;

	newScore = Number(newScore);

	if ( isNaN(newScore) || (newScore>10) || (newScore<0) ) {
		alert('Invalid score value, must be between 1 and 10');
	} else {
		document.getElementById("scorediv"+id).style.display = 'none';
		$.post("/includes/ajax.inc.php?t=33", {mid:manga_id,score:newScore}, function(data){
			document.getElementById("score"+id).innerText = newScore;
			document.getElementById("scoretext"+id).value = '';
		});
	}
}

function anime_updateScore(entry_id)
{
	var newScore = document.getElementById("scoretext"+entry_id).value;
	$.post("/includes/ajax.inc.php?t=63", {id:entry_id,score:newScore}, function(data){
		///joinalert(data);
		document.getElementById("scoreval"+entry_id).innerText = newScore;
		document.getElementById("scoretext"+entry_id).value = '';
		document.getElementById("scorediv"+entry_id).style.display = 'none';
	});
}

function anime_scoreDisplay(id)
{
	var obj = document.getElementById("scorediv"+id);
	$(obj).toggle();
	document.getElementById("scoretext"+id).focus();
}

function anime_checkScoreEnter(e,id)
{
	var key;

     if(window.event)
          key = window.event.keyCode;     //IE
     else
          key = e.which;     //firefox

	if (key == 13)
	 	anime_updateScore(id);
     else
          return true;
}

function getExpand(series_id,colorType)
{
	var moreObject = $('#more'+series_id);
	var memberId = $('#listUserId').val();

	if (moreObject.css('display') == 'block') {		// Hide if loaded
		moreObject.hide();
		return false;
	}

	if (moreObject.html().length>10) {				// Show if date is already loaded
		moreObject.show();
		return false;
	}

	$.post("/includes/ajax-no-auth.inc.php?t=6", {color:colorType,id:series_id,memId:memberId,type:$('#listType').val()}, function(data__safe) {
		moreObject.html(data__safe.html).show();
	}, "json");
}

window.MAL.SNSFunc.initializeFacebook();

function fancyConfirm(message, yes_text, no_text, callback) {
	var result = false;
	$.fancybox({
		'content': '<div style="font-family: verdana, arial; font-size: 11px; text-align: center;">' + message + '<div style="margin-top: 5px;"><input type="button" id="fancybox-confirm-yes-button" value="' + yes_text + '">&nbsp;&nbsp;<input type="button" id="fancybox-confirm-no-button" value="' + no_text + '"></div></div>',
		'autoScale': true,
		'autoDimensions': true,
		"onComplete": function () {
			$("#fancybox-confirm-yes-button").on("click", function () {
				result = true;
				$.fancybox.close();
			});
			$("#fancybox-confirm-no-button").on("click", function () {
				result = false;
				$.fancybox.close();
			});
		},
		"onClosed": function () {
			callback(result);
		}
	});
}
